from Vector_DB_Memory import VectorDBMemory
from autogen_core.memory import MemoryContent
import asyncio

memory = VectorDBMemory(collection_name="GeoFile")

async def main():
        await memory.clear()
        await memory.close()

if __name__ == "__main__":
    asyncio.run(main())