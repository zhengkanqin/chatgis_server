# GeoFile/Tools/SpatialQueryTool.py
import os
from datetime import datetime
from typing import Dict, Union

import geopandas as gpd


def spatial_query_tool(
        origin_gdf: gpd.GeoDataFrame,
        query_gdf: gpd.GeoDataFrame,
        relation: str = "intersects",
        save_shp: bool = True,
        save_geojson: bool = True
) -> Dict[str, Union[int, str]]:
    """
    执行空间查询并保存结果

    参数:
        origin_gdf: 待查询的数据集 (GeoDataFrame)
        query_gdf: 空间查询对象 (GeoDataFrame)
        relation: 空间关系类型 (可选，默认为"intersects")
            - 支持: "intersects", "contains", "within", "touches", "crosses", "overlaps"
        save_shp: 是否保存Shapefile (可选，默认为True)
        save_geojson: 是否保存GeoJSON (可选，默认为True)

    返回:
        包含以下字段的字典:
        {
            "result_num": 匹配要素数量,
            "result_shp_path": Shapefile保存路径,
            "result_geojson_path": GeoJSON保存路径
        }

    异常:
        当输入无效或空间查询失败时抛出ValueError
    """
    # 验证输入
    if not isinstance(origin_gdf, gpd.GeoDataFrame) or not isinstance(query_gdf, gpd.GeoDataFrame):
        raise ValueError("Both inputs must be GeoDataFrames")

    if origin_gdf.empty or query_gdf.empty:
        raise ValueError("Input GeoDataFrames cannot be empty")

    # 检查坐标系一致性
    if origin_gdf.crs != query_gdf.crs:
        query_gdf = query_gdf.to_crs(origin_gdf.crs)

    # 创建结果目录
    output_dir = os.path.abspath("GeoFile/Result")
    os.makedirs(output_dir, exist_ok=True)

    # 生成唯一文件名（使用时间戳）
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"spatial_query_{timestamp}"
    shp_path = os.path.join(output_dir, f"{base_name}.shp")
    geojson_path = os.path.join(output_dir, f"{base_name}.geojson")

    # 执行空间查询
    try:
        result_gdf = _perform_spatial_query(origin_gdf, query_gdf, relation)
    except Exception as e:
        raise ValueError(f"Spatial query failed: {str(e)}")

    # 保存结果
    result_shp_path = ""
    result_geojson_path = ""

    if save_shp and not result_gdf.empty:
        try:
            # 保存Shapefile（处理字段名截断问题）
            result_gdf.to_file(shp_path, encoding='utf-8')
            result_shp_path = shp_path
            print(f"Saved Shapefile to: {shp_path}")
        except Exception as e:
            print(f"Warning: Failed to save Shapefile: {str(e)}")

    if save_geojson and not result_gdf.empty:
        # 保存GeoJSON（保留所有属性）
        result_gdf.to_file(geojson_path, driver='GeoJSON', encoding='utf-8')
        result_geojson_path = geojson_path
        print(f"Saved GeoJSON to: {geojson_path}")

    # 返回结果
    return {
        "result_num": len(result_gdf),
        "result_shp_path": result_shp_path,
        "result_geojson_path": result_geojson_path
    }


def _perform_spatial_query(
        origin_gdf: gpd.GeoDataFrame,
        query_gdf: gpd.GeoDataFrame,
        relation: str
) -> gpd.GeoDataFrame:
    """
    执行实际的空间查询

    支持的空间关系:
        'intersects', 'contains', 'within', 'touches', 'crosses', 'overlaps'
    """
    # 创建查询几何（合并所有查询几何）
    if len(query_gdf) > 1:
        query_geom = query_gdf.unary_union
    else:
        query_geom = query_gdf.geometry.iloc[0]

    # 根据空间关系类型进行查询
    relation = relation.lower()

    if relation == "intersects":
        mask = origin_gdf.geometry.intersects(query_geom)
    elif relation == "contains":
        mask = origin_gdf.geometry.contains(query_geom)
    elif relation == "within":
        mask = origin_gdf.geometry.within(query_geom)
    elif relation == "touches":
        mask = origin_gdf.geometry.touches(query_geom)
    elif relation == "crosses":
        mask = origin_gdf.geometry.crosses(query_geom)
    elif relation == "overlaps":
        mask = origin_gdf.geometry.overlaps(query_geom)
    else:
        raise ValueError(f"Unsupported spatial relation: {relation}")

    # 应用过滤
    result_gdf = origin_gdf[mask].copy()

    # 添加空间关系信息
    result_gdf["query_relation"] = relation

    # 添加查询对象ID（如果原始数据有唯一ID）
    if "query_id" in query_gdf.columns:
        result_gdf["query_id"] = query_gdf["query_id"].iloc[0] if len(query_gdf) == 1 else "multiple"

    return result_gdf
