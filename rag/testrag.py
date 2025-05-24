from Vector_DB_Memory import VectorDBMemory
from autogen_core.memory import MemoryContent
import asyncio

memory = VectorDBMemory(collection_name="my_chat")

async def main():
    try:
        # 添加内容
        print("正在添加内容到向量数据库...")
        content1 = MemoryContent(
            content="这是一段测试内容1",
            mime_type="text/plain",
            metadata={"timestamp": "2024-04-21", "speaker": "user1"}
        )
        content2 = MemoryContent(
            content="这是另一段测试内容2",
            mime_type="text/plain",
            metadata={"timestamp": "2024-04-21", "speaker": "user2"}
        )
        await memory.add(content1)
        await memory.add(content2)
        
        # 查看所有内容
        print("\n查看所有内容：")
        all_contents = await memory.get_all()
        for i, content in enumerate(all_contents, 1):
            print(f"\n内容 {i}:")
            print(f"文本: {content.content}")
            print(f"元数据: {content.metadata}")
        
        # 查询内容
        print("\n正在查询相关内容...")
        query_result = await memory.query("测试")
        
        # 打印查询结果
        print("\n查询结果：")
        for i, result in enumerate(query_result.results, 1):
            print(f"\n结果 {i}:")
            print(f"内容: {result.content}")
            print(f"元数据: {result.metadata}")
            
        # 清空数据库（取消注释以测试）
        # print("\n清空数据库...")
        # await memory.clear()
        
    finally:
        # 确保数据库正确关闭
        await memory.close()

if __name__ == "__main__":
    asyncio.run(main())