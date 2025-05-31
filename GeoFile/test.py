import asyncio
import json

from GeoFile.Service.ToolService import shp_service


async def main():
    # 测试转换操作（convert）
    # convert_result = await shp_service.ainvoke({
    #     "file_path": "GeoFile/AAATestFile/Shp/防火站.shp",
    #     "operation": "convert",
    #     "params": {
    #         "attributes": [],  # 空列表表示全部属性
    #         # "output_path": "GeoFile/Result"  # 可选的
    #     }
    # })
    # print(json.dumps(convert_result, indent=2, ensure_ascii=False))

    # 测试属性查询操作（query）
    query_result = await shp_service.ainvoke({
        "file_path": "GeoFile/AAATestFile/Shp/防火站.shp",
        "operation": "query",
        "params": {
            "query_target": "all",
            # "target_ids": [1]  # 假设查询ID为1的要素
        }
    })
    print(json.dumps(query_result, indent=2, ensure_ascii=False))

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
