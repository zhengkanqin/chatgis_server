# GeoFile/Tools/BufferQueryTool.py
from typing import List

import geopandas as gpd
from pyproj import CRS

from GeoFile.Tools.TableExportTool import TableExporterFactory


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


def buffer_query_tool(file_path: str,
                      buffer_create_ids: List[int],
                      query_file_path: str,
                      target_ids: List[int],
                      buffer_distance: float):
    """
    为指定要素创建缓冲区，并检查目标要素是否在缓冲区内

    参数:
    - file_path: 缓冲区创建要素的SHP文件路径
    - buffer_create_ids: 用于创建缓冲区的要素ID列表
    - query_file_path: 目标要素的SHP文件路径
    - target_ids: 需要检查的目标要素ID列表
    - buffer_distance: 缓冲区距离(米)
    """
    # 读取两个SHP文件
    gdf_buffer = gpd.read_file(file_path)
    gdf_target = gpd.read_file(query_file_path)

    # 验证输入
    if not buffer_create_ids:
        raise ValueError("必须提供至少一个缓冲区创建要素ID")
    if not target_ids:
        raise ValueError("必须提供至少一个目标要素ID")
    if buffer_distance <= 0:
        raise ValueError("缓冲区距离必须大于0")

    # 检查ID范围
    buffer_invalid_ids = [id for id in buffer_create_ids if id < 0 or id >= len(gdf_buffer)]
    target_invalid_ids = [id for id in target_ids if id < 0 or id >= len(gdf_target)]

    if buffer_invalid_ids:
        raise ValueError(f"缓冲区创建要素中存在无效ID: {buffer_invalid_ids}")
    if target_invalid_ids:
        raise ValueError(f"目标要素中存在无效ID: {target_invalid_ids}")

    # 检查坐标系一致性
    if gdf_buffer.crs != gdf_target.crs:
        # 如果坐标系不同，将目标要素转换到缓冲区创建要素的坐标系
        gdf_target = gdf_target.to_crs(gdf_buffer.crs)
        print(
            f"警告: 两个文件的坐标系不同({gdf_buffer.crs} vs {gdf_target.crs})，已将目标要素转换到缓冲区创建要素的坐标系")

    # 提取缓冲区创建要素和目标要素
    buffer_features = gdf_buffer.iloc[buffer_create_ids]
    target_features = gdf_target.iloc[target_ids].copy()

    # 合并缓冲区创建要素的几何
    combined_geometry = buffer_features.unary_union

    # 检查坐标系并创建缓冲区
    if gdf_buffer.crs is None or not gdf_buffer.crs.is_projected:
        # 地理坐标系转换为投影坐标系
        centroid = combined_geometry.centroid
        utm_crs = _get_utm_crs(centroid.x, centroid.y)

        # 转换坐标系
        gdf_buffer_proj = gdf_buffer.to_crs(utm_crs)
        buffer_proj = gdf_buffer_proj.iloc[buffer_create_ids].unary_union

        # 在投影坐标系中创建缓冲区
        buffer_geom = buffer_proj.buffer(buffer_distance)

        # 转换回原始坐标系
        buffer_geom = gpd.GeoSeries([buffer_geom], crs=utm_crs).to_crs(gdf_buffer.crs).iloc[0]
    else:
        # 投影坐标系直接创建缓冲区
        buffer_geom = combined_geometry.buffer(buffer_distance)

    # 检查目标要素是否在缓冲区内
    target_features['in_buffer'] = target_features.geometry.intersects(buffer_geom)

    return TableExporterFactory.export(target_features, title="缓冲区查询结果")
