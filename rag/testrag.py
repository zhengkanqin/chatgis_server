from Vector_DB_Memory import VectorDBMemory
from autogen_core.memory import MemoryContent
import asyncio

memory = VectorDBMemory(collection_name="my_chat_history")
content = MemoryContent(
    content="这是一段需要记住的1231231对1331231321话内1111容呵呵",
    mime_type="text/plain",
    metadata={"timestamp": "2024-04-21", "speaker": "user1"}
)

async def main():
    try:
        # 添加内容
        print("正在添加内容到向量数据库...")
        await memory.add(content)
        
        # 查询内容
        print("\n正在查询相关内容...")
        query_result = await memory.query("呵呵")
        
        # 打印查询结果
        print("\n查询结果：")
        for i, result in enumerate(query_result.results, 1):
            print(f"\n结果 {i}:")
            print(f"内容: {result.content}")
            print(f"元数据: {result.metadata}")
    finally:
        # 确保数据库正确关闭
        await memory.close()

if __name__ == "__main__":
    asyncio.run(main())