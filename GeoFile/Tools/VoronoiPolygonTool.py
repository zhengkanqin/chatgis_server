# GeoFile/Tools/VoronoiPolygonTool.py
import geopandas as gpd
import numpy as np
from scipy.spatial import Voronoi
from shapely.geometry import Polygon, box


def create_voronoi_polygons(gdf: gpd.GeoDataFrame, buffer_percent=0.1) -> gpd.GeoDataFrame:
    """
    为点集生成泰森多边形（Voronoi图）

    参数:
    - gdf: 包含点几何的GeoDataFrame
    - buffer_percent: 用于创建缓冲区的百分比（防止无限多边形）

    返回:
    - 包含泰森多边形的GeoDataFrame
    """
    # 验证输入是否为点
    if not all(gdf.geometry.type == 'Point'):
        raise ValueError("输入GeoDataFrame必须包含点几何")

    # 计算边界框并创建缓冲区
    minx, miny, maxx, maxy = gdf.total_bounds
    dx = maxx - minx
    dy = maxy - miny
    buffer_size = max(dx, dy) * buffer_percent

    # 创建缓冲多边形
    buffered_bbox = box(
        minx - buffer_size,
        miny - buffer_size,
        maxx + buffer_size,
        maxy + buffer_size
    )

    # 获取点坐标
    points = np.array([(geom.x, geom.y) for geom in gdf.geometry])

    # 使用SciPy计算Voronoi图（处理无限区域）
    vor = Voronoi(points)

    # 创建泰森多边形
    voronoi_polygons = []
    for region_idx in vor.point_region:
        region = vor.regions[region_idx]
        if -1 in region:  # 处理无限区域
            # 找到无限区域的顶点
            vertices = vor.vertices[region]

            # 创建凸包并裁剪到缓冲区
            if len(vertices) > 0:
                poly = Polygon(vertices).convex_hull
                clipped_poly = poly.intersection(buffered_bbox)

                # 只保留多边形部分
                if clipped_poly.geom_type == 'Polygon':
                    voronoi_polygons.append(clipped_poly)
                elif clipped_poly.geom_type == 'MultiPolygon':
                    # 选择最大的多边形部分
                    largest_area = 0
                    largest_poly = None
                    for poly_part in clipped_poly.geoms:
                        if poly_part.area > largest_area:
                            largest_area = poly_part.area
                            largest_poly = poly_part
                    if largest_poly:
                        voronoi_polygons.append(largest_poly)
            else:
                # 空区域 - 使用缓冲区作为回退
                voronoi_polygons.append(buffered_bbox)
        else:
            # 有限区域 - 直接创建多边形
            poly = Polygon(vor.vertices[region])
            voronoi_polygons.append(poly)

    # 创建结果GeoDataFrame
    result_gdf = gpd.GeoDataFrame(
        geometry=voronoi_polygons,
        crs=gdf.crs
    )

    # 添加原始点属性
    for col in gdf.columns:
        if col != 'geometry':  # 跳过几何列
            result_gdf[col] = gdf[col].values

    # 添加原始点作为属性
    result_gdf['original_point'] = gdf.geometry

    return result_gdf
