import asyncio
import json

import geopandas as gpd
import GeoFile.Service.ToolService as ToolService

# GeoFile/AAATestFile/Shp/spatial_query_20250610_191452.shp

async def main():
    # 测试阅读操作（convert）
    # convert_result = await read_file.ainvoke({
    #     "file_path": "GeoFile/AAATestFile/Shp/spatial_query_20250610_191452.shp"
    # })
    # print(json.dumps(convert_result, indent=2, ensure_ascii=False))

    # st = "{'type': 'Polygon', 'path': [[114.20047091643444, 30.66892046532886], [114.29015773855063, 30.655997733455273], [114.21311905801493, 30.58439409666541], [114.20047091643444, 30.66892046532886]]}"
    # parsed_source = safe_json_parse(st)
    # result = _load_geojson_dict(parsed_source)
    # print(result)

    # gdf = gpd.read_file("GeoFile/AAATestFile/Shp/spatial_query_20250610_191452.shp", encoding='utf-8', errors='ignore')
    # result = read_geographic_data("GeoFile/AAATestFile/Shp/购物服务.shp")
    # print(gdf)

    # 测试转换操作（convert）
    convert_result = await ToolService.shp_to_type.ainvoke({
        "file_path": "GeoFile/AAATestFile/Shp/购物服务.shp",
        "type_name": "png",
        "attributes": []
    })
    print(json.dumps(convert_result, indent=2, ensure_ascii=False))

    # 测试属性查询操作（query）
    # query_result = await attribute_query.ainvoke({
    #     "file_path": "GeoFile/AAATestFile/Shp/湖北省县级行政区划.shp",
    #     "query_target": "all"
    # })
    # print(json.dumps(query_result, indent=2, ensure_ascii=False))

    # 测试缓冲区分析（buffer）
    # buffer_result = await shp_service.ainvoke({
    #     "file_path": "GeoFile/AAATestFile/Shp/防火站.shp",
    #     "operation": "buffer",
    #     "params": {
    #         "target_id": 1,
    #         "buffer_distance": 100,  # 100米缓冲区
    #         # "output_path": "可选输出路径"  # 可选的
    #     }
    # })
    # print("\n缓冲区分析结果:", json.dumps(buffer_result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
