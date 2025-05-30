# GeoFile/Tools/BufferTool.py
import os
from datetime import datetime

import geopandas as gpd
from shapely.geometry import Polygon, Point, MultiPolygon
from typing import List, Dict, Any, Optional
from pyproj import CRS, Transformer


def buffer_tool(
        file_path: str,
        target_ids: List[int],
        buffer_distance: float,
        custom_output_path: Optional[str] = None
):
    """
    为SHP文件中的指定要素创建缓冲区

    参数:
    - file_path: SHP文件路径
    - target_ids: 目标要素ID列表
    - buffer_distance: 缓冲区距离(米)
    - output_path: 输出路径(可选)
    """
    # 读取SHP文件
    gdf = gpd.read_file(file_path)

    # 验证输入
    if not target_ids:
        raise ValueError("必须提供至少一个目标要素ID")
    if buffer_distance <= 0:
        raise ValueError("缓冲区距离必须大于0")

    # 检查ID范围
    invalid_ids = [id for id in target_ids if id < 0 or id >= len(gdf)]
    if invalid_ids:
        raise ValueError(f"无效的要素ID: {invalid_ids}")

    # 提取目标要素
    target_features = gdf.iloc[target_ids]

    # 合并目标要素几何
    combined_geometry = target_features.unary_union

    # 检查坐标系并创建缓冲区
    if gdf.crs is None or not gdf.crs.is_projected:
        # 如果是地理坐标系，需要转换为投影坐标系进行缓冲区分析
        centroid = combined_geometry.centroid
        utm_crs = _get_utm_crs(centroid.x, centroid.y)

        # 转换坐标系
        gdf_proj = gdf.to_crs(utm_crs)
        target_proj = gdf_proj.iloc[target_ids].unary_union

        # 在投影坐标系中创建缓冲区
        buffer_geom = target_proj.buffer(buffer_distance)

        # 转换回原始坐标系
        buffer_geom = gpd.GeoSeries([buffer_geom], crs=utm_crs).to_crs(gdf.crs).iloc[0]
    else:
        # 已经是投影坐标系，直接创建缓冲区
        buffer_geom = combined_geometry.buffer(buffer_distance)

    # 创建缓冲区GeoDataFrame
    buffer_gdf = gpd.GeoDataFrame(geometry=[buffer_geom], crs=gdf.crs)

    # 9. 计算边界框 (左上右下)
    minx, miny, maxx, maxy = buffer_geom.bounds
    bbox = [minx, miny, maxx, maxy]  # [左, 下, 右, 上]

    # 10. 保存SHP文件 (如果指定了输出路径)
    # 处理输出路径
    if custom_output_path:
        # 使用自定义的完整输出路径
        output_dir = os.path.abspath(custom_output_path)
        # 确保目录存在
        if output_dir:  # 如果路径包含目录
            os.makedirs(output_dir, exist_ok=True)
    else:
        # 创建默认结果目录
        output_dir = os.path.abspath("GeoFile/Result")
        os.makedirs(output_dir, exist_ok=True)

    # 生成唯一的输出文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = os.path.splitext(os.path.basename(file_path))[0]

    # 保存GeoJSON文件
    geojson_path = os.path.join(output_dir, f"{base_name}_buffer_{timestamp}.geojson")
    buffer_gdf.to_file(geojson_path, driver="GeoJSON")
    geojson_saved_path = os.path.abspath(geojson_path)

    # 保存SHP文件
    shp_path = os.path.join(output_dir, f"{base_name}_buffer_{timestamp}.shp")
    buffer_gdf.to_file(shp_path, driver="ESRI Shapefile")
    shp_saved_path = os.path.abspath(shp_path)

    # 11. 返回结果
    return geojson_saved_path, shp_saved_path


def _get_utm_crs(lon: float, lat: float) -> CRS:
    """
    根据经纬度获取合适的UTM投影坐标系

    参数:
    - lon: 经度
    - lat: 纬度

    返回:
    - UTM坐标系
    """
    # 计算UTM区域编号
    utm_zone = int((lon + 180) / 6) + 1

    # 确定半球 (北半球 or 南半球)
    hemisphere = "north" if lat >= 0 else "south"

    # 创建UTM坐标系
    return CRS.from_dict({
        'proj': 'utm',
        'zone': utm_zone,
        'ellps': 'WGS84',
        'datum': 'WGS84',
        'units': 'm',
        'no_defs': True,
        hemisphere: True
    })