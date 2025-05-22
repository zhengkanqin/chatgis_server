# backend/GeoDataProcessor.py

from connection_manager import manager
from autogen_core.tools import FunctionTool
import pandas as pd
import os
import json
import logging
import numpy as np
from typing import Annotated, Optional
from pydantic import Field
from datetime import datetime


def classify_field_type(dtype, data):
    """字段类型分类逻辑"""
    # 尝试识别日期类型
    try:
        if pd.to_datetime(data, errors='raise').notna().all():
            return "Date"
    except:
        pass

    if np.issubdtype(dtype, np.floating):
        return "Double" if dtype == np.float64 else "Float"
    elif np.issubdtype(dtype, np.integer):
        min_val = data.min()
        max_val = data.max()
        if -32768 <= min_val and max_val <= 32767:
            return "Short Integer"
        return "Long Integer"
    elif dtype == object:
        if data.apply(lambda x: isinstance(x, str)).all():
            return "Text"
        return "Unknown"
    return "Unknown"


async def process_geo_data_file(
        file_path: Annotated[
            str,
            Field(description="需要处理的数据文件路径，支持Excel(.xlsx/.xls)和文本文件(.txt/.csv)",
                  example="./data/points.xlsx")
        ],
        lon_col: Annotated[
            Optional[str],
            Field(description="经度字段列名/列索引（无表头时使用数字索引）", example="lng")
        ] = None,
        lat_col: Annotated[
            Optional[str],
            Field(description="纬度字段列名/列索引（无表头时使用数字索引）", example="lat")
        ] = None
):
    """
    处理包含地理坐标点的多源数据文件，自动识别或指定经纬度字段，提取空间分布特征和属性统计信息

    参数:
    - file_path: 数据文件路径
    - lon_col: 经度字段标识（支持列名或列索引）
    - lat_col: 纬度字段标识（支持列名或列索引）

    返回:
    - 处理结果状态及地理特征摘要
    """
    try:
        # ================= 文件验证 =================
        if not os.path.exists(file_path):
            err_msg = f"文件 {file_path} 不存在"
            await manager.send_message(err_msg)
            return {"status": "error", "message": err_msg}

        file_ext = os.path.splitext(file_path)[1].lower()

        # ================= 数据加载 =================
        if file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
        elif file_ext in ['.txt', '.csv']:
            sep = '\t' if file_ext == '.txt' else ','
            df = pd.read_csv(file_path, sep=sep, engine='python')
        else:
            err_msg = f"不支持的文件格式：{file_ext}"
            await manager.send_message(err_msg)
            return {"status": "error", "message": err_msg}

        # ================= 坐标字段检测 =================
        coordinate_info = []
        header_mode = not df.columns.str.contains('^Unnamed').all()

        # 自动检测坐标字段（当未指定时）
        auto_detect_cols = {
            'lon': ['lng', 'longitude', '经度', 'x'],
            'lat': ['lat', 'latitude', '纬度', 'y']
        }

        def detect_col(target_type):
            if locals()[f"{target_type}_col"] is not None:
                return locals()[f"{target_type}_col"]

            for candidate in auto_detect_cols[target_type]:
                if candidate in df.columns:
                    return candidate
            return None

        lon_col = detect_col('lon') or (0 if not header_mode else None)
        lat_col = detect_col('lat') or (1 if not header_mode else None)

        # ================= 智能识别增强 =================
        if lon_col is None or lat_col is None:
            # 获取所有数值列
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()

            # 中国地理坐标范围阈值
            CHINA_LON_RANGE = (73.66, 135.05)  # 东经73°40′~135°05′
            CHINA_LAT_RANGE = (18.15, 53.55)  # 北纬18°10′~53°33′

            # 遍历数值列寻找候选列
            candidate_pairs = []
            for i in range(len(numeric_cols) - 1):
                col1, col2 = numeric_cols[i], numeric_cols[i + 1]

                # 检查列对是否相邻（处理带表头和无表头情况）
                is_adjacent = (
                        (isinstance(col1, int) and isinstance(col2, int) and col2 == col1 + 1) or
                        (isinstance(col1, str) and isinstance(col2, str) and
                         col2.isdigit() and col1.isdigit() and int(col2) == int(col1) + 1)
                )

                if not is_adjacent:
                    continue

                # 检查数值范围
                col1_data = df[col1].dropna()
                col2_data = df[col2].dropna()

                col1_lon_candidate = (col1_data.between(*CHINA_LON_RANGE).mean() > 0.95)
                col2_lat_candidate = (col2_data.between(*CHINA_LAT_RANGE).mean() > 0.95)

                col1_lat_candidate = (col1_data.between(*CHINA_LAT_RANGE).mean() > 0.95)
                col2_lon_candidate = (col2_data.between(*CHINA_LON_RANGE).mean() > 0.95)

                # 记录符合条件的列对
                if col1_lon_candidate and col2_lat_candidate:
                    candidate_pairs.append((col1, col2))
                elif col1_lat_candidate and col2_lon_candidate:
                    candidate_pairs.append((col2, col1))

            # 选择最佳候选对
            if candidate_pairs:
                # 优先选择置信度最高的列对
                best_pair = max(candidate_pairs, key=lambda pair: (
                        df[pair[0]].between(*CHINA_LON_RANGE).mean() +
                        df[pair[1]].between(*CHINA_LAT_RANGE).mean()
                ))
                lon_col, lat_col = best_pair
                logging.info(f"智能选择坐标列：{lon_col}(经度), {lat_col}(纬度)")
            else:
                err_msg = "无法自动识别坐标字段，请手动指定lon_col和lat_col参数"
                await manager.send_message(err_msg)
                return {"status": "error", "message": err_msg}

        # 验证坐标字段有效性
        for col, col_type in [(lon_col, "经度"), (lat_col, "纬度")]:
            if col is None:
                err_msg = f"未检测到{col_type}字段，请明确指定列名/索引"
                await manager.send_message(err_msg)
                return {"status": "error", "message": err_msg}

            try:
                df[col] = pd.to_numeric(df[col])
            except:
                err_msg = f"{col_type}字段 {col} 包含非数值数据"
                await manager.send_message(err_msg)
                return {"status": "error", "message": err_msg}

        # ================= 数据分析 =================
        analysis = {
            "file_info": {
                "file_name": os.path.basename(file_path),
                "file_type": file_ext.strip('.'),
                "total_points": len(df),
                "coordinates_range": {
                    "min_lon": df[lon_col].min(),
                    "max_lon": df[lon_col].max(),
                    "min_lat": df[lat_col].min(),
                    "max_lat": df[lat_col].max()
                }
            },
            "attributes": {
                "fields": {},
                "system_fields": []
            }
        }

        # 处理每个字段
        for col in df.columns:
            if col in [lon_col, lat_col]:
                continue  # 跳过坐标字段

            dtype = df[col].dtype
            field_type = classify_field_type(dtype, df[col])
            stats = {}

            # 数值型处理
            if field_type in ["Double", "Float", "Short Integer", "Long Integer"]:
                stats = {
                    "type": field_type,
                    "min": df[col].min(),
                    "max": df[col].max(),
                    "mean": df[col].mean()
                }
            # 日期型处理
            elif field_type == "Date":
                stats = {
                    "type": "Date",
                    "min": df[col].min().strftime("%Y-%m-%d"),
                    "max": df[col].max().strftime("%Y-%m-%d")
                }
            # 文本型处理
            elif field_type == "Text":
                unique_values = df[col].dropna().unique()
                stats = {
                    "type": "Text",
                    "unique_count": len(unique_values),
                    "sample_values": unique_values.tolist()[:3] if len(unique_values) <= 3 else None
                }
            # 系统字段检测
            elif col.lower() in ['id', 'fid', 'oid']:
                analysis['attributes']['system_fields'].append({
                    "name": col,
                    "type": "Long Integer" if field_type in ["Short Integer", "Long Integer"] else field_type,
                    "count": len(df[col].unique())
                })
                continue

            analysis['attributes']['fields'][col] = stats

        # ================= 消息生成 =================
        output = [
            f"- 地理数据处理完成：{analysis['file_info']['file_name']}",
            "  - 数据概况：",
            f"    - 文件类型：{analysis['file_info']['file_type'].upper()}",
            f"    - 总点数：{analysis['file_info']['total_points']:,}",
            "  - 坐标范围：",
            f"    - 经度：{analysis['file_info']['coordinates_range']['min_lon']:.4f} ~ "
            f"{analysis['file_info']['coordinates_range']['max_lon']:.4f}",
            f"    - 纬度：{analysis['file_info']['coordinates_range']['min_lat']:.4f} ~ "
            f"{analysis['file_info']['coordinates_range']['max_lat']:.4f}",
            "  - 属性字段："
        ]

        # 添加字段信息
        for col, stats in analysis['attributes']['fields'].items():
            if stats['type'] in ["Double", "Float", "Short Integer", "Long Integer"]:
                output.append(
                    f"    - 数值字段 [{col}]（{stats['type']}）："
                    f"平均 {stats['mean']:.2f} | 最大 {stats['max']} | 最小 {stats['min']}"
                )
            elif stats['type'] == "Date":
                output.append(
                    f"    - 日期字段 [{col}]：范围 {stats['min']} 至 {stats['max']}"
                )
            elif stats['type'] == "Text":
                if stats['unique_count'] <= 3:
                    values = ', '.join(map(str, stats['sample_values']))
                    output.append(f"    - 文本字段 [{col}]：包含值 {values}")
                else:
                    output.append(f"    - 文本字段 [{col}]：唯一值数量 {stats['unique_count']}")

        # 添加系统字段
        for field in analysis['attributes']['system_fields']:
            output.append(
                f"    - 系统字段 [{field['name']}]：{field['type']}类型，共{field['count']}条记录"
            )

        # ================= 结果推送 =================
        result_msg = "\n".join(output)
        await manager.send_message(result_msg)

        logging.info(f"地理数据处理摘要：{json.dumps(analysis, indent=2)}")

        return {"status": "success", "data": result_msg}

    except Exception as e:
        error_msg = f"文件处理失败：{str(e)}"
        await manager.send_message(error_msg)
        logging.error(f"{error_msg}\n{str(e)}", exc_info=True)
        return {"status": "error", "message": error_msg}

geo_data_processor_tool = FunctionTool(
    process_geo_data_file,
    name="process_geo_data",
    description="处理包含地理坐标点的多源数据文件（Excel/TXT/CSV），自动识别或指定经纬度字段，提取空间分布特征和属性统计信息，生成可视化分析报告",
)