# GeoFile/Common/ErrorsHandler/ShpOperationErrors.py
"""
Shp文件操作异常处理模块

为Shp文件操作中出现的异常提供合适的处理
"""
import logging
import os
import shutil
import tempfile
import geopandas as gpd

from pyproj.exceptions import CRSError
from pyogrio.errors import DataSourceError
from pandas.errors import EmptyDataError, ParserError

from connection_manager import manager


class ShpBaseErrorHandler:
    """异常处理基类"""
    ERROR_TYPE = Exception  # 基类默认处理所有异常

    def __init__(self, file_path, operation, params, error_obj):
        self.file_path = file_path
        self.operation = operation
        self.params = params
        self.error_obj = error_obj
        self.error_info = {
            "原因": "未知错误",
            "技术诊断": [],
            "修复建议": []
        }

    def build_error_info(self):
        """构建错误信息（子类必须实现）"""
        self.error_info.update({
            "原因": "未知错误",
            "技术诊断": [
                f"原始错误: {str(self.error_obj)}",
            ],
            "修复建议": []
        })

    async def format_response(self):
        """统一格式化输出"""
        self.build_error_info()
        sections = [
            f"■ 错误原因\n{self.error_info['原因']}",
            "▼ 技术诊断\n" + "\n".join(self.error_info["技术诊断"]),
            "⚙ 修复建议\n" + "\n".join(self.error_info["修复建议"])
        ]
        return "\n".join(sections)


class FileNotFoundHandler(ShpBaseErrorHandler):
    """文件不存在异常处理"""
    ERROR_TYPE = FileNotFoundError

    def build_error_info(self):
        self.error_info.update({
            "原因": "文件路径不存在",
            "技术诊断": [
                f"请求路径: {self.file_path}",
                f"系统报错: {str(self.error_obj)}",
                "可能原因:",
                "1. 文件路径包含特殊字符",
                "2. 文件已被移动或删除",
                "3. 使用了错误的相对路径"
            ],
            "修复建议": [
                "1. 检查路径中的中文字符或空格",
                "2. 尝试使用绝对路径",
                "3. 验证文件是否存在于指定位置"
            ]
        })


class ValueErrorHandler(ShpBaseErrorHandler):
    """数值或参数异常处理"""
    ERROR_TYPE = ValueError

    def build_error_info(self):
        error_msg = str(self.error_obj).lower()
        reasons = []
        solutions = []

        if "操作类型" in error_msg:
            reasons.append(error_msg)
            solutions.append("请使用支持的操作方式！")
        elif "文件类型" in error_msg:
            reasons.append(error_msg)
            solutions.append("仅支持使用Shp文件格式！")
        elif "属性字段" in error_msg:
            reasons.append(error_msg)
            solutions.append("属性字段不存在，请重新阅读文件以确定属性字段名称是否正确！")
        elif "查询目标类型" in error_msg:
            reasons.append(error_msg)
            solutions.append("请检查对应参数是否是四种查询中的一种！")
        else:
            reasons.append(error_msg)
            solutions.append("请检查该值是否合规！")

        self.error_info.update({
            "原因": "文件重要值错误",
            "技术诊断": reasons,
            "修复建议": solutions
        })


class ShpOperationErrorFactory:
    """异常处理工厂"""
    HANDLERS = {
        handler.ERROR_TYPE: handler
        for handler in [
            FileNotFoundHandler,
            ValueErrorHandler
        ]
    }

    @classmethod
    def get_handler(cls, file_path, operation, params, error_obj):
        """获取匹配的处理器"""
        for err_class, handler in cls.HANDLERS.items():
            if isinstance(error_obj, err_class):
                return handler(file_path, operation, params, error_obj)
        return ShpBaseErrorHandler(file_path, operation, params, error_obj)
