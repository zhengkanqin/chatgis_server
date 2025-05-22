# backend/ShpProcessor.py

from connection_manager import manager
from autogen_core.tools import FunctionTool
import geopandas as gpd
import os
import json
import logging
import numpy as np
from typing import Annotated
from pydantic import Field
from datetime import datetime


def classify_field_type(dtype, data):
    """详细字段类型分类判断"""
    if np.issubdtype(dtype, np.floating):
        if dtype == np.float32:
            return "Float"
        return "Double"
    elif np.issubdtype(dtype, np.integer):
        min_val = data.min()
        max_val = data.max()
        if -32768 <= min_val and max_val <= 32767:
            return "Short Integer"
        return "Long Integer"
    elif np.issubdtype(dtype, np.datetime64) or isinstance(data.iloc[0], datetime):
        return "Date"
    elif dtype == object:
        sample = data.dropna().iloc[0] if not data.empty else ""
        if isinstance(sample, str) and len(sample.encode('utf-8')) < 254:
            return "Text"
        return "BLOB"  # 实际shapefile不支持，保留识别能力
    return "Unknown"


async def read_shp_file( file_path:str ):
    """
    读取并解析shapefile地理数据文件，提取关键地理信息特征

    参数:
    - file_path: 需要读取的shp文件路径

    返回:
    - 处理结果状态及关键特征摘要
    """
    try:
        # 验证文件存在性
        if not os.path.exists(file_path):
            err_msg = f"文件 {file_path} 不存在"
            await manager.send_message(err_msg)
            return {"status": "error", "message": err_msg}

        # 读取shp文件
        gdf = gpd.read_file(file_path)

        # 计算坐标范围
        bounds = gdf.total_bounds
        coord_range = {
            "min_lon": bounds[0],
            "max_lon": bounds[2],
            "min_lat": bounds[1],
            "max_lat": bounds[3]
        }

        # 构建特征摘要
        summary = {
            "file_info": {
                "file_name": os.path.basename(file_path),
                "crs": str(gdf.crs),
                "geometry_type": gdf.geometry.type.unique().tolist(),
                "total_features": len(gdf),
                "coord_range": coord_range
            },
            "attributes": {
                "fields": {},
                "special_fields": {
                    "Geometry": [],
                    "GUID": [],
                    "ObjectID": []
                }
            }
        }

        # 分析每个字段
        for col in gdf.columns:
            if col == 'geometry':
                summary['attributes']['special_fields']['Geometry'].append({
                    "type": "Geometry",
                    "count": len(gdf)
                })
                continue

            field_type = classify_field_type(gdf[col].dtype, gdf[col])
            stats = {}

            # 数值型处理
            if field_type in ["Float", "Double", "Short Integer", "Long Integer"]:
                stats = {
                    "type": field_type,
                    "min": gdf[col].min(),
                    "max": gdf[col].max(),
                    "mean": gdf[col].mean()
                }
            # 文本型处理
            elif field_type == "Text":
                unique_values = gdf[col].dropna().unique()
                stats = {
                    "type": "Text",
                    "unique_count": len(unique_values),
                    "sample_values": unique_values.tolist()[:5] if len(unique_values) <= 5 else None
                }
            # 日期型处理
            elif field_type == "Date":
                stats = {
                    "type": "Date",
                    "min": gdf[col].min().strftime("%Y-%m-%d"),
                    "max": gdf[col].max().strftime("%Y-%m-%d")
                }
            # 特殊字段处理
            elif col.lower() in ['fid', 'objectid']:
                summary['attributes']['special_fields']['ObjectID'].append({
                    "type": "Long Integer",
                    "count": len(gdf[col].unique())
                })
                continue

            summary['attributes']['fields'][col] = stats

        # 格式化输出消息
        output = [
            f"- 地理数据处理完成：{summary['file_info']['file_name']}",
            "  - 数据概况：",
            f"    - 文件类型：SHP",
            f"    - 坐标系：{summary['file_info']['crs']}",
            f"    - 几何类型：{', '.join(summary['file_info']['geometry_type'])}",
            f"    - 要素总数：{summary['file_info']['total_features']}",
            "  - 坐标范围：",
            f"    - 经度：{summary['file_info']['coord_range']['min_lon']:.4f} ~ {summary['file_info']['coord_range']['max_lon']:.4f}",
            f"    - 纬度：{summary['file_info']['coord_range']['min_lat']:.4f} ~ {summary['file_info']['coord_range']['max_lat']:.4f}",
            "  - 属性字段分析："
        ]

        # 添加字段详细信息
        for col, stats in summary['attributes']['fields'].items():
            if stats['type'] in ["Float", "Double", "Short Integer", "Long Integer"]:
                output.append(
                    f"    - 数值字段 [{col}]（{stats['type']}）："
                    f"平均 {stats['mean']:.2f} | 最大 {stats['max']} | 最小 {stats['min']}"
                )
            elif stats['type'] == "Text":
                if stats['unique_count'] <= 5:
                    values = ', '.join(map(str, stats['sample_values']))
                    output.append(f"    - 文本字段 [{col}]：包含值 {values}")
                else:
                    output.append(f"    - 文本字段 [{col}]：唯一值数量 {stats['unique_count']}")
            elif stats['type'] == "Date":
                output.append(
                    f"    - 日期字段 [{col}]：范围 {stats['min']} 至 {stats['max']}"
                )

        # 添加特殊字段
        for field_type, entries in summary['attributes']['special_fields'].items():
            for entry in entries:
                output.append(
                    f"    - 系统字段 [{field_type}]：{entry['type']}类型，共{entry['count']}条记录"
                )

        # 发送处理结果
        result_msg = "\n".join(output)
        await manager.send_message(result_msg)

        # 记录原始数据
        logging.info(f"SHP处理原始数据：{json.dumps(summary, indent=2)}")

        return {"status": "success", "data": summary}

    except Exception as e:
        error_msg = f"处理shp文件时发生错误：{str(e)}"
        await manager.send_message(error_msg)
        logging.error(error_msg, exc_info=True)
        return {"status": "error", "message": error_msg}


read_shp_tool = FunctionTool(
    read_shp_file,
    name="read_shapefile",
    description="读取并解析shapefile地理数据文件，提取坐标系、几何类型、属性字段统计等关键特征信息，并将分析结果发送至前端展示",
)