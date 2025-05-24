# backend/ShpProcessor.py

from connection_manager import manager
from autogen_core.tools import FunctionTool
from pyproj.exceptions import CRSError
from pyogrio.errors import DataSourceError
import geopandas as gpd
import os
import json
import logging
import shutil
import tempfile
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


async def read_shp_file(file_path: str):
    """
    读取并解析地理数据文件，提取关键地理信息特征

    参数:
    - file_path: 需要读取的文件路径

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
        try:
            gdf = gpd.read_file(file_path)
        except CRSError as crs_err:
            # 尝试自动修复无效的EPSG代码
            try:
                gdf = await handle_crs_error(file_path, crs_err, manager)
            except Exception as read_error:
                # 生成详细错误报告
                error_info = {
                    "原因": "坐标系修复失败",
                    "技术诊断": [
                        f"初次错误: {str(crs_err)}",
                        f"移除PRJ后错误: {str(read_error)}",
                        "可能原因:",
                        "1. 数据本身坐标值异常",
                        "2. 几何数据损坏",
                        "3. 需要手动指定坐标系"
                    ],
                    "修复建议": [
                        "终极方案：强制指定坐标系参数",
                        "操作步骤：",
                        "1. 用文本编辑器查看坐标值范围",
                        "2. 根据数据来源推测正确坐标系",
                        "3. 使用QGIS重新定义投影"
                    ]
                }

                err_msg = format_crs_error(file_path, error_info)
                await manager.send_message(err_msg)
                logging.error(f"CRS Repair Failed: {str(read_error)}")
                return {"status": "error", "message": err_msg}

        except DataSourceError as data_err:
            return await handle_datasource_error(file_path, data_err, manager)

        except FileNotFoundError as e:
            error_info = {
                "原因": "文件路径不存在或已被移动",
                "建议": [
                    "1. 检查文件路径是否包含中文字符或特殊字符",
                    "2. 确认文件后缀名与实际格式一致（如.shp/.geojson）",
                    "3. 尝试使用绝对路径代替相对路径"
                ]
            }
            err_msg = f"文件读取失败：{str(e)}\n{format_error(error_info)}"

        except PermissionError as e:
            error_info = {
                "原因": "文件访问权限不足",
                "建议": [
                    "1. 检查文件是否被其他程序占用（如Excel、GIS软件）",
                    "2. 右键文件属性→安全→添加用户读写权限",
                    "3. 尝试将文件复制到有写入权限的目录"
                ]
            }
            err_msg = f"权限错误：{str(e)}\n{format_error(error_info)}"

        except UnicodeDecodeError as e:
            error_info = {
                "原因": "文件编码不兼容",
                "建议": [
                    "1. 尝试指定编码参数：gpd.read_file(file_path, encoding='gbk')",
                    "2. 用文本编辑器检查文件头部的编码格式",
                    "3. 将文件另存为UTF-8编码格式"
                ]
            }
            err_msg = f"编码错误：{str(e)}\n{format_error(error_info)}"

        except Exception as e:
            error_info = {
                "原因": "未知数据解析错误",
                "建议": [
                    "1. 检查文件是否完整（特别关注ZIP压缩包）",
                    "2. 尝试用gpd.read_file(file_path, driver='GeoJSON')指定驱动",
                    "3. 提供文件样本给技术人员分析"
                ]
            }
            err_msg = f"数据解析失败：{str(e)}\n{format_error(error_info)}"

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
            "  - 属性字段："
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

        return {"status": "success", "data": result_msg}

    except Exception as e:
        error_msg = f"处理shp文件时发生错误：{str(e)}"
        await manager.send_message(error_msg)
        logging.error(error_msg, exc_info=True)
        return {"status": "error", "message": error_msg}


async def handle_crs_error(file_path, error, manager):
    """专门处理坐标系错误"""
    error_info = {
        "原因": "坐标系定义异常",
        "具体诊断": [
            f"原始错误: {str(error)}",
            "可能原因:",
            "1. PRJ文件缺失或损坏",
            "2. 使用了非标准EPSG代码",
            "3. 数据导出时未正确设置投影"
        ],
        "修复建议": [
            "方案一：使用QGIS修复坐标系",
            "   1. 在QGIS中右键图层 → 设置坐标系",
            "   2. 导出为新的shapefile",
            "方案二：手动修复PRJ文件",
            "   用文本编辑器创建同名的.prj文件",
            "   插入标准WKT坐标系定义"
        ]
    }

    # 生成详细错误报告
    err_msg = (
        f"🚨 坐标系配置错误: {os.path.basename(file_path)}\n"
        f"▌{format_error(error_info)}\n"
        "💡 正在尝试自动修复……"
    )

    await manager.send_message(err_msg)
    logging.error(f"CRS Error: {str(error)}")

    """处理坐标系错误并尝试自动修复.prj文件问题"""
    prj_path = os.path.splitext(file_path)[0] + ".prj"
    temp_prj = None
    prj_removed = False

    # 尝试备份并移除.prj文件
    if os.path.exists(prj_path):
        temp_prj = tempfile.NamedTemporaryFile(delete=False, suffix=".prj")
        shutil.move(prj_path, temp_prj.name)
        prj_removed = True
        logging.info(f"已临时移除PRJ文件: {prj_path} → {temp_prj.name}")

    # 尝试重新读取数据（无.prj文件状态）
    gdf = gpd.read_file(file_path)

    await manager.send_message(
        f"成功读取文件 {os.path.basename(file_path)}\n"
        f"- 移除无效PRJ文件后坐标系: {gdf.crs}"
    )

    return gdf


async def handle_datasource_error(file_path, error, manager):
    """处理数据源错误"""
    error_info = {
        "原因": "数据源读取失败",
        "具体诊断": analyze_datasource_error(error),
        "修复建议": [
            "步骤1：检查文件完整性（必须包含.shp/.shx/.dbf等）",
            "步骤2：验证文件编码：使用文本编辑器查看是否有乱码",
            "步骤3：尝试指定驱动参数：gpd.read_file(file_path, driver='ESRI Shapefile')",
            "步骤4：使用QGIS打开文件验证数据有效性"
        ]
    }

    err_msg = (
        f"🔧 数据源错误: {os.path.basename(file_path)}\n"
        f"▌{format_error(error_info)}\n"
        "💡 可尝试修复命令：/fix_datasource"
    )

    await manager.send_message(err_msg)
    logging.error(f"DataSource Error: {str(error)}")
    return {"status": "error", "message": err_msg}


def analyze_datasource_error(error):
    """智能分析数据源错误原因"""
    error_msg = str(error).lower()
    reasons = []

    if "no such file" in error_msg:
        reasons.append("文件路径包含中文字符或特殊符号")
    elif "unrecognized data source" in error_msg:
        reasons.append("文件扩展名与实际格式不匹配")
    elif "failed to open" in error_msg:
        reasons.extend(["文件正在被其他程序占用", "文件权限不足"])
    elif ".shx" in error_msg:
        reasons.append("Shapefile组件不完整（缺少.shx文件）")

    return reasons or ["未知数据源错误，需要进一步诊断"]


def format_error(error_info):
    """增强的错误格式化"""
    sections = [f"■ 错误原因\n{error_info['原因']}", "▼ 技术诊断\n" + "\n".join(error_info["具体诊断"]),
                "⚙ 修复方案\n" + "\n".join(error_info["修复建议"])]
    return "\n\n".join(sections)


def format_crs_error(file_path, error_info):
    """格式化坐标系错误信息"""
    return (
        f"🚨 坐标系修复失败: {os.path.basename(file_path)}\n"
        f"■ 错误原因\n{error_info['原因']}\n\n"
        f"▼ 技术诊断\n" + "\n".join(error_info["技术诊断"]) + "\n\n"
        f"⚙ 修复建议\n" + "\n".join(error_info["修复建议"])
    )


read_tool = FunctionTool(
    read_shp_file,
    name="read_shapefile",
    description="读取并解析shapefile地理数据文件，提取坐标系、几何类型、属性字段统计等关键特征信息，并将分析结果发送至前端展示",
)
