# GeoFile/Tools/GeographicObjectTool.py
import json
import os
import re
from typing import Union, Dict, Any, Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString, Polygon, shape

from Agent.Globals import UserLayers


def read_geographic_data(
        source: Union[str, Dict],
        condition: Optional[Union[str, Dict[str, Any]]] = None
) -> gpd.GeoDataFrame:
    """
    读取地理数据并返回GeoDataFrame，支持多种输入格式和条件过滤

    参数:
        source: 地理数据源，可以是:
            - 文件路径: SHP/GeoJSON等地理文件路径
            - GeoJSON对象: {'type': 'Polygon', 'coordinates': [...]}
            - 图层引用: "[$layer]图层名[$layer]"
            - 缓冲区参数: {'type': 'buffer', 'source': 源要素, 'distance': 距离(米)}\
            - 其他对象: 尝试转为JSON字典处理

        condition: 源数据属性过滤条件(可选)
            - 格式: {"属性名": 值} 或 {"属性名": 操作函数} 或 {"属性名": [值1, 值2]}
            - 示例:
                {"type": "building"}  # 简单等于条件
                {"height": lambda h: h > 20}  # 函数表达式条件
                {"name": ["学校", "医院"]}  # 包含在列表中

    返回:
        geopandas.GeoDataFrame对象
    """
    # 处理非str/dict类型的输入 (如对象)
    if not isinstance(source, (str, dict)):
        try:
            # 尝试将对象转换为JSON字典
            source = json.loads(json.dumps(source, default=lambda o: o.__dict__))
        except Exception as e:
            raise TypeError(f"无法转换对象为JSON: {type(source)}. 错误: {str(e)}") from e

    # 处理图层引用
    if isinstance(source, str) and re.match(r"^\[\$layer].*\[\$layer]$", source):
        layer_name = source[8:-8]  # 移除[$layer]标记
        gdf = _load_layer(layer_name)

    # 处理字符串形式的输入（可能是文件路径或JSON字符串）
    elif isinstance(source, str):
        try:
            # 尝试将字符串解析为JSON对象
            parsed_source = safe_json_parse(source)
            if isinstance(parsed_source, dict):
                # 如果是字典类型，则根据内容进一步处理
                if parsed_source.get('type') == 'buffer':
                    gdf = _create_buffer(parsed_source)
                else:
                    gdf = _load_geojson_dict(parsed_source)
            else:
                # 不是字典则当作文件路径处理
                gdf = _load_file(source)
        except json.JSONDecodeError:
            # JSON解析失败，当作普通文件路径
            gdf = _load_file(source)

    # 处理直接传入的字典对象
    elif isinstance(source, dict):
        if source.get('type') == 'buffer':
            gdf = _create_buffer(source)
        else:
            gdf = _load_geojson_dict(source)

    else:
        raise TypeError(f"Unsupported source type: {type(source)}. "
                        "Expected str (file path, layer reference or JSON) or dict.")

    # 应用条件过滤
    if condition:
        gdf = _apply_condition(gdf, condition)

    return gdf


def _load_file(path: str) -> gpd.GeoDataFrame:
    """加载文件路径指向的地理数据"""
    # 检查文件是否存在
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    # 根据文件扩展名判断类型
    ext = os.path.splitext(path)[1].lower()

    # 支持的矢量格式
    vector_formats = [".shp", ".geojson", ".json", ".gpkg", ".kml", ".gml",
                      ".sqlite", ".db", ".tab", ".mif", ".dxf", ".vrt"]

    if ext in vector_formats:
        # 尝试不同编码读取矢量文件
        # encodings_to_try = ['utf-8', 'GB18030', 'gbk', 'big5', 'latin1', 'cp1252']
        # for encoding in encodings_to_try:
        try:
            gdf = gpd.read_file(path, encoding="utf-8")
        except UnicodeDecodeError:
            gdf = gpd.read_file(path, encoding="GB18030")
    else:
        raise ValueError(f"Unsupported file extension: {ext}. "
                         "Supported: .shp, .geojson, .json")

    return _ensure_valid_gdf(gdf)


def _load_geojson_dict(geojson_dict: Dict) -> gpd.GeoDataFrame:
    """处理GeoJSON字典对象并转换为有效的GeoDataFrame"""
    # 处理不同类型的GeoJSON结构
    if geojson_dict.get('type') == 'FeatureCollection':
        # 直接处理FeatureCollection
        gdf = gpd.GeoDataFrame.from_features(geojson_dict)

    elif geojson_dict.get('type') == 'Feature':
        # 处理单个Feature
        geometry = shape(geojson_dict['geometry'])
        properties = geojson_dict.get('properties', {})
        gdf = gpd.GeoDataFrame([properties], geometry=[geometry])

    elif geojson_dict.get('type') in ['Point', 'LineString', 'Polygon',
                                      'MultiPoint', 'MultiLineString', 'MultiPolygon']:
        # 处理纯几何对象
        geometry = shape(geojson_dict)
        gdf = gpd.GeoDataFrame(geometry=[geometry])

    else:
        # 尝试处理非标准GeoJSON格式
        try:
            # 尝试作为FeatureCollection处理
            gdf = gpd.GeoDataFrame.from_features(geojson_dict)
        except Exception:
            # 尝试作为单个几何对象处理
            try:
                geometry = shape(geojson_dict)
                gdf = gpd.GeoDataFrame(geometry=[geometry])
            except Exception as e:
                raise ValueError(f"无法识别的GeoJSON结构: {geojson_dict.get('type')}") from e

    return _ensure_valid_gdf(gdf)


def _load_layer(layer_name: str) -> gpd.GeoDataFrame:
    """从图层仓库加载图层数据"""
    # 从图层仓库获取数据
    layer_data = next(
        (layer for layer in UserLayers if layer.get('name') == layer_name),
        None
    )
    if not layer_data:
        raise ValueError(f"Layer not found: {layer_name}")

    # 根据图层类型处理数据
    layer_type = layer_data['data']['type']

    if layer_type == 'Marker':
        point = Point(layer_data['data']['position'])
        gdf = gpd.GeoDataFrame(geometry=[point], crs="EPSG:4326")

    elif layer_type == 'Polyline':
        line = LineString(layer_data['data']['path'])
        gdf = gpd.GeoDataFrame(geometry=[line], crs="EPSG:4326")

    elif layer_type in ['Polygon', 'Boundary', '面', '边界']:
        polygon = Polygon(layer_data['data']['path'])
        gdf = gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")

    elif layer_type == 'Circle':
        center = Point(layer_data['data']['center'])
        radius = layer_data['data']['radius']

        # 创建缓冲区（圆形）
        buffer_params = {
            'type': 'buffer',
            'source': {'type': 'Point', 'coordinates': center},
            'distance': radius
        }
        gdf = _create_buffer(buffer_params)

    elif layer_type == 'GeoJSON':
        gdf = _load_geojson_dict(layer_data['data'])

    else:
        raise ValueError(f"Unsupported layer type: {layer_type}")

    return _ensure_valid_gdf(gdf)


def _create_buffer(buffer_params: Dict) -> gpd.GeoDataFrame:
    """创建缓冲区几何"""
    # 获取源要素
    source = buffer_params['source']
    distance = buffer_params['distance']

    # 加载源要素
    source_gdf = read_geographic_data(source)

    # 确保源要素是点、线或多边形
    if source_gdf.geometry.iloc[0].geom_type not in ['Point', 'LineString', 'Polygon']:
        raise ValueError("Buffer can only be created from Point, LineString or Polygon geometries")

    # 创建缓冲区（需要投影到平面坐标系）
    projected_gdf = source_gdf.to_crs(epsg=3857)  # Web Mercator
    buffered = projected_gdf.buffer(distance)

    # 转换回WGS84
    buffered_gdf = gpd.GeoDataFrame(geometry=buffered, crs="EPSG:3857")
    buffered_gdf = buffered_gdf.to_crs(epsg=4326)

    return _ensure_valid_gdf(buffered_gdf)


def _apply_condition(gdf: gpd.GeoDataFrame, condition: Union[str, Dict[str, Any]]) -> gpd.GeoDataFrame:
    """
    应用条件过滤到GeoDataFrame

    参数:
        gdf: 要过滤的GeoDataFrame
        condition: 过滤条件字典

    返回:
        过滤后的GeoDataFrame
    """
    if gdf.empty:
        return gdf

    # 创建初始的True掩码
    mask = pd.Series([True] * len(gdf), index=gdf.index)

    if isinstance(condition, str):
        condition = json.loads(condition)

    # 处理每个条件
    for attr, value in condition.items():
        # 检查属性是否存在
        if attr not in gdf.columns:
            raise ValueError(f"Attribute '{attr}' not found in data properties")

        # 处理不同类型的条件
        if callable(value):
            # 函数表达式条件
            try:
                # 尝试直接应用函数
                attr_mask = gdf[attr].apply(value)
            except Exception as e:
                raise ValueError(f"Error applying function to attribute '{attr}': {str(e)}")

            if not attr_mask.dtype == bool:
                raise ValueError(f"Condition function for '{attr}' must return boolean values")

            mask = mask & attr_mask

        elif isinstance(value, list):
            # 列表包含条件
            mask = mask & gdf[attr].isin(value)

        else:
            # 简单等于条件
            mask = mask & (gdf[attr] == value)

    # 应用过滤
    filtered_gdf = gdf[mask].copy()

    return filtered_gdf


def _ensure_valid_gdf(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """确保GeoDataFrame有效并设置默认CRS"""
    if not isinstance(gdf, gpd.GeoDataFrame):
        raise TypeError("Failed to create valid GeoDataFrame")

    # 设置默认CRS (WGS84) 如果缺失
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)

    return gdf


def safe_json_parse(json_str):
    # 修复单引号问题
    json_str = json_str.replace("'", '"')

    return json.loads(json_str)
