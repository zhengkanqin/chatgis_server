# GeoFile/Service/ToolService.py
from typing import Optional, List

from langchain_core.tools import tool

from GeoFile.Processors.DataInputProcessor import FileProcessorFactory
from GeoFile.Processors.ShpOperationProcessor import ShpProcessorFactory


@tool()
async def read_file(file_path: str):
    """
    读取并解析地理数据文件，提取坐标系、几何类型、属性字段统计等关键特征信息，支持Shp/Excel/Csv/Txt等多种形式

    参数:
    - file_path: 需要读取的文件路径
    """
    return await FileProcessorFactory.create_processor(file_path)


# @tool()
# async def shp_service(file_path: str, operation: str, params: Optional[Dict[str, Any]] = None):
#     """
#     提供SHP文件的三种核心处理功能：
#
#     1. 格式转换 - 将SHP转换为GeoJSON格式，可选择保留指定属性字段
#        operation='convert', params={'attributes': ['field1','field2'](空列表表示全部属性), 'output_path': 输出路径(可选)}
#
# 2. 属性查询 - 灵活查询SHP文件属性数据： - 要素字段查询(单个或多个要素字段): operation='query', params={'query_target': 'single', 'target_ids': [
# 要素ID], 'attributes': ['字段名']} - 横向查询(单行或多行): operation='query', params={'query_target': 'row', 'target_ids': [
# 要素ID]} - 纵向查询(单列或多列): operation='query', params={'query_target': 'column', 'attributes': ['字段名']} - 全表查询:
# operation='query', params={'query_target': 'all'}
#
# 3. 缓冲区分析 - 为指定要素创建缓冲区，输出: - GeoJSON格式的缓冲区多边形 - 地图瓦片坐标(左上右下) - 保存为新的SHP文件 operation='buffer', params={'target_ids':
# [要素ID], 'buffer_distance': 距离(米), 'buffer_color': 图片颜色(16进制颜色字符串), 'output_path': 输出路径(可选)}
#
#     参数:
#     - file_path: SHP文件路径
#     - operation: 执行的操作类型(convert/query/buffer)
#     - params: 操作参数字典，包含以下可选键:
#         * attributes: 需要处理的属性字段列表(convert/query时有效)
#         * query_target: 查询目标类型(query时有效): 'all'=全表, 'row'=单行, 'column'=单列, 'single'=单格
#         * target_ids: 目标要素ID(query/buffer时有效)
#         * buffer_distance: 缓冲区距离(米)(buffer时有效)
#         * buffer_color: 缓冲区PNG图片颜色(字符串)(buffer时有效)
#         * output_path: 缓冲区输出目录路径(convert/buffer时有效)
#
#     返回:
#     - 操作结果字典，包含:
#         status: "success" 或 "error"
#         message: 错误摘要信息(发生错误时存在)
#         data: 结构化数据(操作成功时存在)
#     """
#     # 确保params是字典类型
#     params = params or {}
#
# # 根据operation类型过滤不需要的参数 if operation == "convert": # 需要attributes/output_path filtered_params = {k: v for k,
# v in params.items() if k in ["attributes", "output_path"]} elif operation == "query": #
# 需要attributes和query_target，target_id仅在query_target='row'时需要 filtered_params = {k: v for k, v in params.items() if k
# in ["attributes", "query_target", "target_ids"]} elif operation == "buffer": #
# 需要target_id/buffer_distance/output_path filtered_params = {k: v for k, v in params.items() if k in ["target_ids",
# "buffer_distance", "buffer_color", "output_path"]} else: # 无效操作类型 return { "status": "error", "message":
# f"不支持的操作类型: {operation}" }
#
#     return await ShpProcessorFactory.create_processor(file_path, operation, filtered_params)


@tool()
async def to_geojson(file_path: str,
                     attributes: Optional[List[str]] = None,
                     output_path: Optional[str] = None
                     ):
    """
    将SHP转换为GeoJSON格式，可选择保留指定属性字段

    参数:
    - file_path: SHP文件路径
    - attributes: 需要处理的属性字段列表(空列表表示全部属性)
    """
    params = {}
    if attributes:
        params.update({"attributes": attributes})
    if output_path:
        params.update({"output_path": output_path})

    return await ShpProcessorFactory.create_processor(file_path, "convert", params)


@tool()
async def attribute_query(file_path: str,
                          query_target: str,
                          target_ids: Optional[List[int]] = None,
                          attributes: Optional[List[str]] = None
                          ):
    """
    灵活查询SHP文件属性数据：
       - 要素字段查询(单个或多个要素字段): query_target = 'single', target_ids = [要素ID], attributes= ['字段名']
       - 横向查询(单行或多行): query_target = 'row', target_ids = [要素ID]
       - 纵向查询(单列或多列): query_target = 'column', attributes = ['字段名']
       - 全表查询: query_target = 'all'

    参数:
    - file_path: SHP文件路径
    - query_target: 查询目标类型: 'all'=全表, 'row'=行查询, 'column'=列查询, 'single'=格查询
    - target_ids: 目标要素ID列表(single/row时有效)
    - attributes: 目标属性字段列表(single/column时有效)
    """
    params = {}
    params.update({"query_target": query_target})
    if attributes:
        params.update({"attributes": attributes})
    if target_ids:
        params.update({"target_ids": target_ids})

    return await ShpProcessorFactory.create_processor(file_path, "query", params)


@tool()
async def buffer_create(file_path: str,
                        buffer_create_ids: List[int],
                        buffer_distance: float,
                        buffer_color: Optional[str] = "#66CCFF",
                        output_path: Optional[str] = None
                        ):
    """
    为指定要素创建缓冲区，输出:
       - GeoJSON格式的缓冲区多边形
       - 保存为新的SHP文件
       - 地图PNG图片和定位点坐标

    参数:
    - file_path: 缓冲区创建要素的SHP文件路径
    - buffer_create_ids: 用于创建缓冲区的要素ID列表
    - buffer_distance: 缓冲区距离(米)
    - buffer_color: 缓冲区PNG图片颜色(16进制颜色代码)
    """
    params = {}
    params.update({"buffer_create_ids": buffer_create_ids})
    params.update({"buffer_distance": buffer_distance})
    params.update({"buffer_color": buffer_color})
    if output_path:
        params.update({"output_path": output_path})

    return await ShpProcessorFactory.create_processor(file_path, "buffer", params)


@tool()
async def buffer_query(file_path: str,
                       buffer_create_ids: List[int],
                    query_file_path: str,
                       target_ids: List[int],
                       buffer_distance: float,
                       buffer_color: Optional[str] = "#66CCFF",
                       output_path: Optional[str] = None
                       ):
    """
    以指定要素创建缓冲区，并查询另一些要素是否处在缓冲区内，输出:
       - GeoJSON格式的缓冲区多边形
       - 保存为新的SHP文件
       - 地图PNG图片和定位点坐标
       - 查询结果

    参数:
    - file_path: 缓冲区创建要素的SHP文件路径
    - buffer_create_ids: 用于创建缓冲区的要素ID列表
    - query_file_path: 目标要素的SHP文件路径
    - target_ids: 需要检查的目标要素ID列表
    - buffer_distance: 缓冲区距离(米)
    - buffer_color: 缓冲区PNG图片颜色(16进制颜色代码)
    """
    params = {}
    params.update({"buffer_create_ids": buffer_create_ids})
    params.update({"query_file_path": query_file_path})
    params.update({"target_ids": target_ids})
    params.update({"buffer_distance": buffer_distance})
    params.update({"buffer_color": buffer_color})
    if output_path:
        params.update({"output_path": output_path})

    return await ShpProcessorFactory.create_processor(file_path, "buffer_query", params)

AnalysisTools = [read_file, to_geojson, attribute_query, buffer_query]
