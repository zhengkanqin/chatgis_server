import asyncio
from GeoFile.ShpProcessor import read_shp_file


async def main():
    abc = await read_shp_file("D:/Temp/data/china/单线铁路.shp")
    print(abc)

if __name__ == "__main__":
    asyncio.run(main())