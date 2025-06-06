# GeoFile/Tools/Shp2TypeTool.py
import os
from datetime import datetime
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
from typing import Tuple, Optional


def shp2geojson(file_path, gdf, attributes, custom_output_path: Optional[str] = None):
    # 如果指定了属性字段，则筛选数据
    if attributes:
        # 确保几何字段被包含
        required_columns = set(attributes) | {'geometry'}
        # 检查请求的属性是否存在
        missing_columns = required_columns - set(gdf.columns)
        if missing_columns:
            raise ValueError(
                f"请求的属性字段不存在: {', '.join(missing_columns)}"
            )
        gdf = gdf[list(required_columns)]

    # 统计信息
    feature_count = len(gdf)
    attribute_count = len(gdf.columns) - 1  # 减去几何字段

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
    output_file = f"{base_name}_{timestamp}.geojson"
    output_path = os.path.join(output_dir, output_file)

    # 保存为GeoJSON文件
    gdf.to_file(output_path, driver="GeoJSON")

    return output_path, feature_count, attribute_count


def shp2png(file_path: str,
            gdf: gpd.GeoDataFrame,
            attributes: Optional[list] = None,
            custom_output_path: Optional[str] = None) -> Tuple[str, Tuple[float, float, float, float]]:
    """
    将SHP文件转换为PNG图像并返回地理坐标范围

    参数:
    - file_path: 原始SHP文件路径
    - gdf: 已加载的GeoDataFrame
    - attributes: 需要高亮显示的属性字段列表
    - custom_output_path: 自定义输出路径

    返回:
    - png_path: 生成的PNG文件路径
    - bbox: 图像的地理坐标范围 (minx, miny, maxx, maxy)
    """
    try:
        # 获取地理范围
        minx, miny, maxx, maxy = gdf.total_bounds
        bbox = (minx-0.01, miny-0.01, maxx+0.01, maxy+0.01)

        # 创建绘图 - 设置透明背景
        fig, ax = plt.subplots(figsize=(10, 10), facecolor='none')  # 透明背景
        ax.set_aspect('equal')
        fig.patch.set_alpha(0)  # 完全透明背景

        # 移除所有坐标轴和边框
        ax.axis('off')
        ax.set_frame_on(False)

        # 根据属性设置样式
        if attributes and len(attributes) > 0:
            # 选择一个属性用于分类着色
            color_by = attributes[0]

            # 数值型属性使用渐变着色
            if gdf[color_by].dtype.kind in 'ifc':
                gdf.plot(column=color_by, ax=ax, legend=True,
                         cmap='viridis', legend_kwds={'shrink': 0.5})
            # 分类属性使用离散着色
            else:
                unique_values = gdf[color_by].unique()
                colors = plt.cm.tab10(np.linspace(0, 1, len(unique_values)))
                for value, color in zip(unique_values, colors):
                    gdf[gdf[color_by] == value].plot(ax=ax, color=color, label=value)
                ax.legend(title=color_by)
        else:
            # 无属性时使用统一颜色
            gdf.plot(ax=ax, color='blue', edgecolor='black')

        # 设置坐标范围
        ax.set_xlim(minx-0.01, maxx+0.01)
        ax.set_ylim(miny-0.01, maxy+0.01)

        # 处理输出路径
        if custom_output_path:
            output_dir = os.path.abspath(custom_output_path)
            os.makedirs(output_dir, exist_ok=True)
        else:
            output_dir = os.path.abspath("GeoFile/Result/Images")
            os.makedirs(output_dir, exist_ok=True)

        # 生成唯一的输出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        png_file = f"{base_name}_{timestamp}.png"
        png_path = os.path.join(output_dir, png_file)

        # 保存为PNG
        plt.savefig(png_path, dpi=600, bbox_inches='tight')
        plt.close(fig)

        return png_path, bbox

    except Exception as e:
        plt.close()
        raise RuntimeError(f"PNG生成失败: {str(e)}")