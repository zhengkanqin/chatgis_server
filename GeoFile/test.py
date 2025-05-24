import asyncio

from GeoFile.Service.DataInputService import read_file


async def main():
    abc = await read_file("GeoFile/AAATestFile/Excel/level2_模糊表头.xlsx")
    print(abc)

if __name__ == "__main__":
    asyncio.run(main())
