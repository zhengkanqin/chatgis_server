# GeoFile/Processors/ShpOperationProcessor.py
"""
SHP文件操作处理模块

支持基础地理信息分析、几何类型统计、坐标范围提取等功能。
"""
import os
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

import geopandas as gpd

from GeoFile.Common.ErrorsHandler.ShpOperationErrors import ShpOperationErrorFactory
from GeoFile.Common.Message import success, error
from GeoFile.Tools.BufferQueryTool import buffer_query_tool
from GeoFile.Tools.BufferTool import buffer_tool
from GeoFile.Tools.Shp2TypeTool import shp2geojson, shp2png
from GeoFile.Tools.ShpQueryTools import query_tool


class BaseOperationProcessor(ABC):
    """文件操作器基类"""

    SUPPORTED_OPERATION = []

    def __init__(self, file_path: str, operation: str, params: Optional[Dict[str, Any]] = None):
        """
        :param file_path: 文件路径（支持绝对/相对路径）
        :param operation: 操作类型
        :param params: 操作参数
        """
        self.file_path = os.path.abspath(file_path)
        self.operation = operation
        self.params = params or {}
        self._validate()

    def _validate(self):
        """基础验证"""
        if not self._check_extension():
            raise ValueError(f"不支持的文件类型: {self.extension}")
        if self.operation not in self.SUPPORTED_OPERATION:
            raise ValueError(f"操作类型'{self.operation}'不被支持")

    @property
    def extension(self) -> str:
        """获取文件扩展名"""
        return os.path.splitext(self.file_path)[1].lower()

    def _check_extension(self) -> bool:
        """检查扩展名是否支持"""
        # 这里应该根据实际支持的文件类型实现
        return self.extension in ['.shp']

    @abstractmethod
    async def core(self):
        """处理入口方法（需子类实现）"""
        pass


class ConvertProcessor(BaseOperationProcessor):
    """Shp2Type转换器"""

    SUPPORTED_OPERATION = ['convert']

    async def core(self):
        gdf = gpd.read_file(self.file_path)
        attributes = self.params.get('attributes', [])
        output_dir = self.params.get('output_path')
        type_name = self.params.get('type_name')

        if type_name == 'geojson':
            output_path, feature_count, attribute_count = shp2geojson(self.file_path, gdf, attributes, output_dir)

            result = (
                f"GeoJSON文件已保存至: {output_path}\n"
                f"要素数量: {feature_count}\n"
                f"属性字段数量: {attribute_count}"
            )
        elif type_name == 'png':
            output_path, bbox = shp2png(self.file_path, gdf, attributes, output_dir)

            # 解构边界框坐标
            minx, miny, maxx, maxy = bbox

            result = (
                f"格式转换文件已保存至: {output_path}\n"
                f"边界框范围: ({minx}, {miny}) 与 ({maxx}, {maxy})之间"
            )
        else:
            raise ValueError(f"暂不支持转换的文件格式: {type_name}")

        return result


class QueryProcessor(BaseOperationProcessor):
    """属性查询处理器"""

    SUPPORTED_OPERATION = ['query']

    async def core(self):
        gdf = gpd.read_file(self.file_path)
        attributes = self.params.get('attributes', [])
        query_target = self.params.get('query_target', 'all')
        target_ids = self.params.get('target_ids', [])

        return query_tool(gdf, query_target, target_ids, attributes)


class BufferProcessor(BaseOperationProcessor):
    """缓冲区分析处理器"""

    SUPPORTED_OPERATION = ['buffer']

    async def core(self):
        buffer_create_ids = self.params.get('buffer_create_ids')
        buffer_distance = self.params.get('buffer_distance')
        buffer_color = self.params.get('buffer_color')
        output_path = self.params.get('output_path')

        geojson_saved_path, png_saved_path, shp_saved_path, bbox = (
            buffer_tool(self.file_path, buffer_create_ids, buffer_distance, buffer_color, output_path))

        # 解构边界框坐标
        minx, miny, maxx, maxy = bbox

        # 格式化结果字符串
        result = (
            f"缓冲区GeoJSON文件已保存至: {geojson_saved_path}\n"
            f"缓冲区PNG文件已保存至: {png_saved_path}\n"  # 添加了缺失的换行符
            f"缓冲区SHP文件已保存至: {shp_saved_path}\n"  # 添加了换行符
            f"缓冲区边界框范围: ({minx}, {miny}) 与 ({maxx}, {maxy})之间"
        )

        return result


class BufferQueryProcessor(BaseOperationProcessor):
    """缓冲区分析处理器"""

    SUPPORTED_OPERATION = ['buffer_query']

    async def core(self):
        buffer_create_ids = self.params.get('buffer_create_ids')
        query_file_path = self.params.get('query_file_path')
        target_ids = self.params.get('target_ids')
        buffer_distance = self.params.get('buffer_distance')
        buffer_color = self.params.get('buffer_color')
        output_path = self.params.get('output_path')

        geojson_saved_path, png_saved_path, shp_saved_path, bbox = (
            buffer_tool(self.file_path, buffer_create_ids, buffer_distance, buffer_color, output_path))

        # 解构边界框坐标
        minx, miny, maxx, maxy = bbox

        buffer_query_result = buffer_query_tool(self.file_path, buffer_create_ids, query_file_path, target_ids,
                                                buffer_distance)

        # 格式化结果字符串
        result = (
            f"缓冲区GeoJSON文件已保存至: {geojson_saved_path}\n"
            f"缓冲区PNG文件已保存至: {png_saved_path}\n"  # 添加了缺失的换行符
            f"缓冲区SHP文件已保存至: {shp_saved_path}\n"  # 添加了换行符
            f"缓冲区边界框范围: ({minx}, {miny}) 与 ({maxx}, {maxy})之间\n"
            f"缓冲区查询成功: {buffer_query_result}"
        )

        return result


class ShpProcessorFactory:
    """文件操作器工厂"""

    OPERATION_PROCESSORS = {
        'convert': ConvertProcessor,
        'query': QueryProcessor,
        'buffer': BufferProcessor
    }

    @classmethod
    async def create_processor(cls, file_path: str, operation: str, params: dict):
        """创建处理器实例"""
        try:
            # 验证操作类型是否支持
            if operation not in cls.OPERATION_PROCESSORS:
                raise ValueError(f"不支持的操作类型: {operation}")

            # 获取对应的处理器类
            processor_class = cls.OPERATION_PROCESSORS[operation]

            # 创建处理器实例
            processor = processor_class(file_path, operation, params)

            # 执行处理逻辑
            process_result = await processor.core()

            # 返回成功结果
            return await success(process_result)

        except Exception as e:
            # 异常处理
            handler = ShpOperationErrorFactory.get_handler(file_path, operation, params, e)
            response = await handler.format_response()
            return await error(response)
