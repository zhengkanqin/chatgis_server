import asyncio
import json

from GeoFile.Service.ToolService import attribute_query, shp_to_type, read_file
from GeoFile.Tools.GeographicObjectTool import _load_geojson_dict


async def main():
    # 测试阅读操作（convert）
    # convert_result = await read_file.ainvoke({
    #     "file_path": "GeoFile/AAATestFile/Shp/边界.shp"
    # })
    # print(json.dumps(convert_result, indent=2, ensure_ascii=False))

    st = '{"type": "Polygon", "coordinates": [[[111,111], [114, 514], [123,123], [13345,1241]]]}'
    parsed_source = json.loads(st)
    result = _load_geojson_dict(parsed_source)
    print(result)
    
    # 测试转换操作（convert）
    # convert_result = await shp_to_type.ainvoke({
    #     "file_path": "GeoFile/AAATestFile/Shp/防火站.shp",
    #     "type_name": "png",
    #     "attributes": []
    # })
    # print(json.dumps(convert_result, indent=2, ensure_ascii=False))

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
