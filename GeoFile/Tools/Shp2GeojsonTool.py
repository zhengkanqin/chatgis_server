# GeoFile/Tools/Shp2GeojsonTool.py
import os
from datetime import datetime
from typing import Optional


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
