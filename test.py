import asyncio
from GeoFile.ShpProcessor import read_shp_file


async def main():
    abc = await read_shp_file("GeoFile/TestFile/Shp/防火站.shp")
    print(abc)

if __name__ == "__main__":
    asyncio.run(main())