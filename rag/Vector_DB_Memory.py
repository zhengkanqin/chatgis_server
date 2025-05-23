from autogen_core.memory import Memory, MemoryContent, UpdateContextResult, MemoryQueryResult
from autogen_core.model_context import ChatCompletionContext
import chromadb
from chromadb.config import Settings
from typing import List
import os
from openai import OpenAI
import json
import hashlib
with open('../config.json', 'r', encoding='utf-8') as configFile:
    config = json.load(configFile)


class VectorDBMemory(Memory):
    def __init__(self, collection_name: str = "autogen_memory"):
        # 从配置文件获取数据库路径
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        db_path = os.path.normpath(os.path.join(base_dir, config["向量数据库路径"]))
        print(f"数据库存储路径: {db_path}")
        
        # 确保数据库目录存在
        os.makedirs(db_path, exist_ok=True)
        
        self.client = chromadb.PersistentClient(path=db_path)
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"}
        )

    async def add(self, content: MemoryContent, cancellation_token=None) -> None:
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

    async def query(self, query: str | MemoryContent, cancellation_token=None, **kwargs) -> MemoryQueryResult:
        # 将查询转换为向量
        query_vector = self._get_embedding(str(query))

        # 执行向量搜索
        results = self.collection.query(
            query_embeddings=[query_vector],
            n_results=5  # 返回最相关的5条结果
        )

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
