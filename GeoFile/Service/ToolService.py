# GeoFile/Service/ToolService.py
from typing import Optional, List, Dict, Any, Union

from langchain_core.tools import tool

from GeoFile.Common.ErrorsHandler import UnifiedErrorFactory
from GeoFile.Common.Message import error
from GeoFile.Processors.DataInputProcessor import FileProcessorFactory
from GeoFile.Processors.ShpOperationProcessor import ShpProcessorFactory
from GeoFile.Processors.SpatialOperationProcessor import SpatialProcessorFactory


@tool()
async def read_file(file_path: str):
    """
    读取并解析地理数据文件，提取关键特征信息，支持Shp/GeoJSON/Excel/Csv/Txt等多种矢量形式

    参数:
    - file_path: 需要读取的文件路径
    """
    try:
        return FileProcessorFactory.create_processor(file_path)
    except Exception as e:
        handler = UnifiedErrorFactory.get_handler("数据录入工具", error_obj=e)
        response = handler.format_response()
        return error(response)


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
async def geo_data_convert(source: Union[str, Dict],
                           type_name: str,
                           attributes: Optional[List[str]] = None,
                           output_path: Optional[str] = None
                           ):
    """
    将各种矢量地理数据转换为其他格式，支持GeoJSON/PNG等格式，可选择保留指定属性字段

    参数:
    - query_source: 数据格式转换对象，支持以下格式：
        * 文件路径(str): SHP/GeoJSON/GPKG等地理文件路径
        * GeoJSON对象(str或Dict): {"type": "Polygon", "coordinates": [...]}
        * 图层引用(str): "[$layer]精确图层名[$layer]"
        * 缓冲区参数(str或Dict): {'type': 'buffer', 'source': 源要素(文件路径或GeoJSON对象), 'distance': 距离(米)}
    - type_name: 转换的目标格式(str):
        'geojson': GeoJSON格式
        'png': PNG格式
        'shp': Shapefile文件集
        'gpkg': GeoPackage格式
        'kml': KML格式
        'geotiff': GeoTIFF文件
    - attributes: 需要处理的属性字段列表(空列表表示全部属性):
        type_name = 'geojson', 'shp', 'gpkg', 'kml': attributes表示转换时需要保留的属性字段
        type_name = "png": attributes表示转换时用以着色的数值或分类字段(空列表表示使用同一颜色)
        type_name = "geotiff": attributes表示用于栅格化的属性(空列表表示二值化)
    """
    params = {'type_name': type_name}
    if attributes:
        params.update({"attributes": attributes})
    if output_path:
        params.update({"output_path": output_path})

    try:
        return ShpProcessorFactory.create_processor(source, "convert", params)
    except Exception as e:
        handler = UnifiedErrorFactory.get_handler("数据格式转换工具", error_obj=e)
        response = handler.format_response()
        return error(response)


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

    try:
        return ShpProcessorFactory.create_processor(file_path, "query", params)
    except Exception as e:
        handler = UnifiedErrorFactory.get_handler("属性查询工具", error_obj=e)
        response = handler.format_response()
        return error(response)


@tool()
async def buffer_create(file_path: str,
                        buffer_create_ids: List[int],
                        buffer_distance: float,
                        buffer_color: Optional[str] = "#66CCFF",
                        output_path: Optional[str] = None
                        ):
    """
    为指定要素创建缓冲区

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

    try:
        return ShpProcessorFactory.create_processor(file_path, "buffer", params)
    except Exception as e:
        handler = UnifiedErrorFactory.get_handler("缓冲区创建工具", error_obj=e)
        response = handler.format_response()
        return error(response)


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
    以指定要素创建缓冲区，并查询另一些要素是否处在缓冲区内

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

    try:
        return ShpProcessorFactory.create_processor(file_path, "buffer_query", params)
    except Exception as e:
        handler = UnifiedErrorFactory.get_handler("缓冲区查询工具", error_obj=e)
        response = handler.format_response()
        return error(response)


@tool()
async def spatial_query(source: Union[str, Dict],
                        query_source: Union[str, Dict],
                        queried_condition: Optional[Union[str, Dict[str, Any]]] = None,
                        relation: Optional[str] = "intersects"):
    """
    执行空间查询：检查源要素是否与查询空间对象存在空间关系

    参数:
    - source: 待查询的数据源，支持以下格式：
        * 文件路径(str): SHP/GeoJSON/GPKG等地理文件路径
        * GeoJSON对象(str或Dict): {'type': 'FeatureCollection', ...}
        * 图层引用(str): "[$layer]精确图层名[$layer]"

    - query_source: 空间查询对象，支持以下格式：
        * 文件路径(str): SHP/GeoJSON/GPKG等地理文件路径
        * GeoJSON对象(str或Dict): {"type": "Polygon", "coordinates": [...]}
        * 图层引用(str): "[$layer]精确图层名[$layer]"
        * 缓冲区参数(str或Dict): {'type': 'buffer', 'source': 源要素(文件路径或GeoJSON对象), 'distance': 距离(米)}

    - queried_condition: 源数据属性过滤条件(可选)
        * 类型: str 或 Dict[str, Any]
        * 格式: {"属性名": 值} 或 {"属性名": 操作函数}
        * 示例:
            {"type": "building"}  # 简单等于条件
            {"height": lambda h: h > 20}  # 函数表达式条件
            {"name": ["学校", "医院"]}  # 包含在列表中
    - relation: 空间关系类型 (可选，默认为"intersects")
            - 支持: "intersects", "contains", "within", "touches", "crosses", "overlaps"
    """
    params = {}
    params.update({"query_source": query_source})
    params.update({"relation": relation})
    if queried_condition:
        params.update({"queried_condition": queried_condition})

    try:
        return SpatialProcessorFactory.create_processor("spatial_query", source, params)
    except Exception as e:
        handler = UnifiedErrorFactory.get_handler("空间查询工具", error_obj=e)
        response = handler.format_response()
        return error(response)


@tool()
async def cluster_analysis(
        source: Union[str, Dict],
        algorithm: Optional[str] = 'kmeans',
        use_attributes: Optional[List[str]] = None,
        n_clusters: Optional[int] = None,
        eps: Optional[float] = None
):
    """
    执行空间聚类分析：对空间数据进行多种聚类算法分析

    参数:
    - source: 待进行聚类分析的数据源，支持以下格式：
        * 文件路径(str): SHP/GeoJSON/GPKG等地理文件路径
        * GeoJSON对象(str或Dict): {'type': 'FeatureCollection', ...}
        * 图层引用(str): "[$layer]精确图层名[$layer]"

    - algorithm: 聚类算法类型 (可选，默认为'kmeans')
        * 支持算法:
            'kmeans' - K均值聚类
            'dbscan' - 基于密度的空间聚类
            'agglomerative' - 层次聚类
            'meanshift' - 均值漂移聚类
            'spectral' - 谱聚类
            'optics' - OPTICS密度聚类

    - use_attributes: 用于聚类的属性字段列表 (可选)
        * 类型: List[str]
        * 示例: ["population", "income", "elevation"]
        * 说明: 默认仅使用空间坐标特征，添加属性字段可实现空间+属性联合聚类

    - n_clusters: 聚类数量 (可选，部分算法必需)
        * 类型: int
        * 适用算法: kmeans, agglomerative, spectral
        * 默认值:
            kmeans: 5
            agglomerative: 5
            spectral: 5

    - eps: DBSCAN算法的邻域半径 (可选，DBSCAN算法必需)
        * 类型: float
        * 适用算法: dbscan
        * 默认值: 0.2
    """
    params = {}
    params.update({"algorithm": algorithm})
    if use_attributes:
        params.update({"attributes": use_attributes})
    if n_clusters:
        params.update({"n_clusters": n_clusters})
    if eps:
        params.update({"eps": eps})

    try:
        return SpatialProcessorFactory.create_processor("cluster_analysis", source, params)
    except Exception as e:
        handler = UnifiedErrorFactory.get_handler("空间聚类工具", error_obj=e)
        response = handler.format_response()
        return error(response)


AnalysisTools = [read_file, geo_data_convert, attribute_query, buffer_query, spatial_query, cluster_analysis]
