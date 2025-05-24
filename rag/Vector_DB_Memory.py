from autogen_core.memory import Memory, MemoryContent, UpdateContextResult, MemoryQueryResult
from autogen_core.model_context import ChatCompletionContext
import chromadb
from chromadb.config import Settings
from typing import List
import os
from openai import OpenAI
import json
import hashlib

# 修改配置文件路径
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(base_dir, 'config.json')
with open(config_path, 'r', encoding='utf-8') as configFile:
    config = json.load(configFile)


class VectorDBMemory(Memory):
    def __init__(self, collection_name: str = "autogen_memory"):
        # 从配置文件获取数据库路径
        db_path = os.path.normpath(os.path.join(base_dir, config["向量数据库路径"]))
        print(f"数据库存储路径: {db_path}")
        
        # 确保数据库目录存在
        os.makedirs(db_path, exist_ok=True)
        
        self.client = chromadb.PersistentClient(path=db_path)
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"}
        )

    async def add(self, content: MemoryContent, filepath: str = None, cancellation_token=None) -> None:
        """
        添加内容到向量数据库
        
        Args:
            content: MemoryContent对象，包含内容和元数据
            filepath: 可选的文件路径，如果提供则会自动添加文件路径和修改时间到元数据中
            cancellation_token: 取消令牌
        """
        # 如果提供了filepath，添加文件路径和修改时间到元数据中
        if filepath:
            import os
            from datetime import datetime
            
            # 确保文件存在
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"文件不存在: {filepath}")
            
            # 获取文件最后修改时间
            file_mtime = os.path.getmtime(filepath)
            
            # 更新元数据
            if content.metadata is None:
                content.metadata = {}
            content.metadata.update({
                "filepath": filepath,
                "timestamp": str(file_mtime)  # 转换为字符串存储
            })
        
        # 将内容转换为向量并存储
        vector = self._get_embedding(content.content)
        # 生成一个基于内容的唯一ID
        content_id = hashlib.md5(str(content.content).encode()).hexdigest()
        
        self.collection.add(
            ids=[content_id],
            embeddings=[vector],
            documents=[str(content.content)],
            metadatas=[content.metadata or {}]
        )

    async def query(self, query: str | MemoryContent, metadata_filter: dict = None, cancellation_token=None, **kwargs) -> MemoryQueryResult:
        """
        查询相关内容，支持语义搜索和元数据过滤
        
        Args:
            query: 查询文本或MemoryContent对象
            metadata_filter: 元数据过滤条件，支持以下格式：
                - 精确匹配: {"speaker": "user1"}
                - 数值范围: {"timestamp": {"$gt": "2024-01-01", "$lt": "2024-12-31"}}
                - 多条件: {"speaker": "user1", "type": "test"}
            cancellation_token: 取消令牌
            **kwargs: 其他参数
            
        Returns:
            MemoryQueryResult: 查询结果
        """
        # 将查询转换为向量
        query_vector = self._get_embedding(str(query))

        # 构建查询参数
        query_params = {
            "query_embeddings": [query_vector],
            "n_results": kwargs.get("n_results", 5)  # 默认返回5条结果
        }
        
        # 如果提供了元数据过滤条件，添加到查询参数中
        if metadata_filter:
            query_params["where"] = metadata_filter

        # 执行向量搜索
        results = self.collection.query(**query_params)

        # 构建返回结果
        memory_contents = []
        for doc, metadata in zip(results['documents'][0], results['metadatas'][0]):
            memory_contents.append(
                MemoryContent(
                    content=doc,
                    mime_type="text/plain",
                    metadata=metadata
                )
            )

        return MemoryQueryResult(results=memory_contents)

    async def clear(self) -> None:
        self.collection.delete(where={})

    async def close(self) -> None:
        # 在新版本中不需要显式调用 persist
        pass

    async def update_context(self, model_context: ChatCompletionContext) -> UpdateContextResult:
        # 获取当前对话的上下文
        messages = await model_context.get_messages()
        last_message = messages[-1].content if messages else ""

        # 查询相关记忆
        query_result = await self.query(last_message)

        # 将相关记忆添加到上下文中
        for memory in query_result.results:
            model_context.add_system_message(str(memory.content))

        return UpdateContextResult(memories=query_result)

    def _get_embedding(self, text: str) -> List[float]:
        client = OpenAI(
            api_key=config["Embedding模型密钥"],
            base_url=config["Embedding模型地址"]
        )
        response = client.embeddings.create(
            model=config["Embedding模型名称"],
            input=text,
            dimensions=1024,
            encoding_format="float"
        )
        return response.data[0].embedding

    async def get_all(self) -> List[MemoryContent]:
        """获取集合中的所有数据"""
        results = self.collection.get()
        
        memory_contents = []
        for doc, metadata in zip(results['documents'], results['metadatas']):
            memory_contents.append(
                MemoryContent(
                    content=doc,
                    mime_type="text/plain",
                    metadata=metadata
                )
            )
        return memory_contents

    async def delete_by_id(self, content_id: str) -> None:
        """根据ID删除特定内容"""
        self.collection.delete(ids=[content_id])
        
    async def delete_by_metadata(self, metadata_filter: dict) -> None:
        """根据元数据条件删除内容"""
        self.collection.delete(where=metadata_filter)
        
    async def delete_by_content(self, content: str) -> None:
        """根据内容删除匹配的文档"""
        # 生成内容的ID
        content_id = hashlib.md5(str(content).encode()).hexdigest()
        self.collection.delete(ids=[content_id])

    async def get_metadata_stats(self) -> dict:
        """
        获取记忆库的元数据统计信息
        
        Returns:
            dict: 包含以下信息：
                - total_count: 总记录数
                - metadata_fields: 所有元数据字段及其唯一值列表
                - collection_info: 集合信息
        """
        # 获取所有数据
        results = self.collection.get()
        
        # 统计信息
        stats = {
            "total_count": len(results['documents']),
            "metadata_fields": {},
            "collection_info": {
                "name": self.collection.name,
                "count": len(results['documents'])
            }
        }
        
        # 统计每个元数据字段的唯一值
        for metadata in results['metadatas']:
            for key, value in metadata.items():
                if key not in stats["metadata_fields"]:
                    stats["metadata_fields"][key] = set()
                stats["metadata_fields"][key].add(str(value))
        
        # 将集合转换为列表
        for key in stats["metadata_fields"]:
            stats["metadata_fields"][key] = sorted(list(stats["metadata_fields"][key]))
            
        return stats

    async def get_paginated_data(self, page: int = 1, page_size: int = 100, metadata_filter: dict = None) -> dict:
        """
        分页获取数据
        
        Args:
            page: 页码，从1开始
            page_size: 每页记录数
            metadata_filter: 元数据过滤条件
            
        Returns:
            dict: 包含以下信息：
                - total: 总记录数
                - total_pages: 总页数
                - current_page: 当前页码
                - page_size: 每页记录数
                - data: 当前页的数据列表
        """
        # 获取所有数据
        results = self.collection.get()
        
        # 应用元数据过滤
        filtered_data = []
        if metadata_filter:
            for doc, metadata in zip(results['documents'], results['metadatas']):
                # 检查是否满足所有过滤条件
                if all(metadata.get(k) == v for k, v in metadata_filter.items()):
                    filtered_data.append((doc, metadata))
        else:
            filtered_data = list(zip(results['documents'], results['metadatas']))
        
        # 计算分页信息
        total = len(filtered_data)
        total_pages = (total + page_size - 1) // page_size
        
        # 确保页码有效
        page = max(1, min(page, total_pages))
        
        # 计算当前页的数据范围
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total)
        
        # 获取当前页的数据
        current_page_data = filtered_data[start_idx:end_idx]
        
        # 构建返回结果
        return {
            "total": total,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": page_size,
            "data": [
                {
                    "content": doc,
                    "metadata": metadata
                }
                for doc, metadata in current_page_data
            ]
        }

    async def list_modified_data(self) -> dict:
        """
        列出所有被修改的数据
        
        Returns:
            dict: 包含以下信息：
                - total: 总记录数
                - modified_data: 被修改的数据列表，按修改时间倒序排列
                每个数据项包含：
                    - content: 内容
                    - metadata: 元数据
                    - file_path: 文件路径
                    - last_modified: 最后修改时间
        """
        import os
        from datetime import datetime
        
        # 获取所有数据
        results = self.collection.get()
        
        # 存储修改过的数据
        modified_data = []
        
        # 遍历所有数据
        for doc, metadata in zip(results['documents'], results['metadatas']):
            # 检查是否包含必要的元数据
            if 'timestamp' in metadata and 'filepath' in metadata:
                file_path = metadata['filepath']
                
                # 检查文件是否存在
                if os.path.exists(file_path):
                    # 获取文件最后修改时间
                    file_mtime = os.path.getmtime(file_path)
                    
                    # 如果文件修改时间与元数据中的时间戳不同，说明文件被修改过
                    if abs(file_mtime - float(metadata['timestamp'])) > 1:  # 允许1秒的误差
                        modified_data.append({
                            "content": doc,
                            "metadata": metadata,
                            "file_path": file_path,
                            "last_modified": datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d %H:%M:%S')
                        })
        
        # 按最后修改时间倒序排序
        modified_data.sort(key=lambda x: x['last_modified'], reverse=True)
        
        return {
            "total": len(modified_data),
            "modified_data": modified_data
        }
