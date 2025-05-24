# GeoFile/Service/DataInputService.py
import logging

from autogen_core.tools import FunctionTool
from GeoFile.Processors.DataInputProcessor import FileProcessorFactory


async def read_file(file_path: str):
    """
    读取并解析地理数据文件，提取关键地理信息特征

    参数:
    - file_path: 需要读取的文件路径

    返回:
    - 处理结果状态及关键特征摘要
    """
    return await FileProcessorFactory.create_processor(file_path)

read_tool = FunctionTool(
    read_file,
    name="read_file",
    description="读取并解析地理数据文件，提取坐标系、几何类型、属性字段统计等关键特征信息，支持shp格式",
)
