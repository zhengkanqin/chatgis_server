import asyncio

from GeoFile.Service.DataInputService import read_file


async def main():
    abc = await read_file("GeoFile/AAATestFile/Shp/防火站.shp")
    print(abc)

if __name__ == "__main__":
    asyncio.run(main())
