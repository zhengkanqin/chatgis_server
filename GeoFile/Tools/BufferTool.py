# GeoFile/Tools/BufferTool.py
import os
from datetime import datetime
from typing import List, Optional

import geopandas as gpd
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon as MplPolygon
from pyproj import CRS

from GeoFile.Tools.GeographicObjectTool import read_geographic_data

# 确保使用Agg后端，避免GUI依赖
matplotlib.use('Agg')


def buffer_tool(
        file_path: str,
        buffer_create_ids: List[int],
        buffer_distance: float,
        buffer_color: str,
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
    gdf = read_geographic_data(file_path)

    # 验证输入
    if not buffer_create_ids:
        raise ValueError("必须提供至少一个目标要素ID")
    if buffer_distance <= 0:
        raise ValueError("缓冲区距离必须大于0")

    # 验证颜色格式
    if not buffer_color.startswith('#') or len(buffer_color) not in [4, 7, 9]:
        raise ValueError("颜色格式应为十六进制，如 #RRGGBB 或 #RRGGBBAA")

    # 检查ID范围
    invalid_ids = [ids for ids in buffer_create_ids if ids < 0 or ids >= len(gdf)]
    if invalid_ids:
        raise ValueError(f"无效的要素ID: {invalid_ids}")

    # 提取目标要素
    target_features = gdf.iloc[buffer_create_ids]

    # 合并目标要素几何
    combined_geometry = target_features.unary_union

    # 检查坐标系并创建缓冲区
    if gdf.crs is None or not gdf.crs.is_projected:
        # 如果是地理坐标系，需要转换为投影坐标系进行缓冲区分析
        centroid = combined_geometry.centroid
        utm_crs = _get_utm_crs(centroid.x, centroid.y)

        # 转换坐标系
        gdf_proj = gdf.to_crs(utm_crs)
        target_proj = gdf_proj.iloc[buffer_create_ids].unary_union

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
    bbox = [minx, miny, maxx, maxy]

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

    # 生成PNG图片
    png_path = os.path.join(output_dir, f"{base_name}_buffer_{timestamp}.png")
    create_buffer_png(buffer_geom, buffer_color, png_path)
    png_saved_path = os.path.abspath(png_path)

    # 11. 返回结果
    return geojson_saved_path, png_saved_path, shp_saved_path, bbox


def create_buffer_png(buffer_geom, buffer_color: str, output_path: str) -> None:
    """
    创建缓冲区PNG图片

    参数:
    - buffer_geom: 缓冲区的几何对象
    - buffer_color: 缓冲区填充颜色（十六进制格式）
    - output_path: 输出PNG文件路径
    """
    # 创建图形和坐标轴
    fig, ax = plt.subplots(figsize=(10, 10))

    # 设置透明背景
    fig.patch.set_alpha(0.0)
    ax.set_axis_off()

    # 获取几何边界
    minx, miny, maxx, maxy = buffer_geom.bounds
    width = maxx - minx
    height = maxy - miny

    # 设置坐标轴范围（添加5%的边距）
    margin = max(width, height) * 0.05
    ax.set_xlim(minx - margin, maxx + margin)
    ax.set_ylim(miny - margin, maxy + margin)

    # 确保等比例缩放
    ax.set_aspect('equal')

    # 创建补丁列表
    patches = []

    # 处理不同几何类型
    if buffer_geom.geom_type == 'Polygon':
        # 单个多边形
        patches.append(create_polygon_patch(buffer_geom, buffer_color))
    elif buffer_geom.geom_type == 'MultiPolygon':
        # 多个多边形
        for polygon in buffer_geom.geoms:
            patches.append(create_polygon_patch(polygon, buffer_color))

    # 添加所有补丁到坐标轴
    collection = PatchCollection(patches, match_original=True)
    ax.add_collection(collection)

    # 保存为PNG（透明背景）
    plt.savefig(
        output_path,
        format='png',
        dpi=300,
        bbox_inches='tight',
        pad_inches=0,
        transparent=True
    )

    # 清理资源
    plt.close(fig)


def create_polygon_patch(polygon, color: str) -> MplPolygon:
    """
    为单个多边形创建matplotlib补丁，支持带孔洞的多边形

    参数:
    - polygon: Shapely多边形对象
    - color: 填充颜色（十六进制格式）

    返回:
    - matplotlib.patches.Polygon
    """
    # 获取外部环坐标
    exterior = np.array(polygon.exterior.coords)

    # 获取所有孔洞坐标
    interiors = [np.array(interior.coords) for interior in polygon.interiors]

    # 创建多边形补丁 - 统一处理带孔洞和不带孔洞的情况
    patch = MplPolygon(
        exterior,
        holes=interiors if interiors else None,
        closed=True,
        fill=True,
        edgecolor='none',  # 无边框
        facecolor=color,
        alpha=1.0
    )

    return patch


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
