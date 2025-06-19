# GeoFile/Processors/ShpOperationProcessor.py
"""
SHP文件操作处理模块

支持基础地理信息分析、几何类型统计、坐标范围提取等功能。
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Union

from GeoFile.Common.Message import success
from GeoFile.Tools.BufferQueryTool import buffer_query_tool
from GeoFile.Tools.BufferTool import buffer_tool
from GeoFile.Tools.Gdf2TypeTool import ConverterFactory
from GeoFile.Tools.GeographicObjectTool import read_geographic_data
from GeoFile.Tools.ShpQueryTools import query_tool


class BaseOperationProcessor(ABC):
    """文件操作器基类"""

    SUPPORTED_OPERATION = []

    def __init__(self, source: Union[str, Dict], operation: str, params: Optional[Dict[str, Any]] = None):
        """
        :param source: 文件源
        :param operation: 操作类型
        :param params: 操作参数
        """
        self.source = source
        self.gdf = read_geographic_data(source)
        self.operation = operation
        self.params = params or {}
        self._validate()

    def _validate(self):
        """基础验证"""
        if self.operation not in self.SUPPORTED_OPERATION:
            raise ValueError(f"操作类型'{self.operation}'不被支持")

    @abstractmethod
    def core(self):
        """处理入口方法（需子类实现）"""
        pass


class ConvertProcessor(BaseOperationProcessor):
    """Shp2Type转换器"""

    SUPPORTED_OPERATION = ['convert']

    def core(self):
        # 使用工厂创建转换器
        converter = ConverterFactory.get_converter(
            type_name=self.params.get('type_name'),
            gdf=self.gdf,
            attributes=self.params.get('attributes', []),
            custom_output_path=self.params.get('output_path')
        )

        return converter.convert()


class QueryProcessor(BaseOperationProcessor):
    """属性查询处理器"""

    SUPPORTED_OPERATION = ['query']

    def core(self):
        gdf = self.gdf
        attributes = self.params.get('attributes', [])
        query_target = self.params.get('query_target', 'all')
        target_ids = self.params.get('target_ids', [])

        return query_tool(gdf, query_target, target_ids, attributes)


class BufferProcessor(BaseOperationProcessor):
    """缓冲区分析处理器"""

    SUPPORTED_OPERATION = ['buffer']

    def core(self):
        buffer_create_ids = self.params.get('buffer_create_ids')
        buffer_distance = self.params.get('buffer_distance')
        buffer_color = self.params.get('buffer_color')
        output_path = self.params.get('output_path')

        geojson_saved_path, png_saved_path, shp_saved_path, bbox = (
            buffer_tool(self.gdf, buffer_create_ids, buffer_distance, buffer_color, output_path))

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

    def core(self):
        buffer_create_ids = self.params.get('buffer_create_ids')
        query_file_path = self.params.get('query_file_path')
        target_ids = self.params.get('target_ids')
        buffer_distance = self.params.get('buffer_distance')
        buffer_color = self.params.get('buffer_color')
        output_path = self.params.get('output_path')

        geojson_saved_path, png_saved_path, shp_saved_path, bbox = (
            buffer_tool(self.gdf, buffer_create_ids, buffer_distance, buffer_color, output_path))

        # 解构边界框坐标
        minx, miny, maxx, maxy = bbox

        buffer_query_result = buffer_query_tool(self.gdf, buffer_create_ids, read_geographic_data(query_file_path), target_ids,
                                                buffer_distance)

        # 格式化结果字符串
        result = (
            f"缓冲区GeoJSON文件已保存至: {geojson_saved_path}\n"
            f"缓冲区PNG文件已保存至: {png_saved_path}\n"
            f"缓冲区SHP文件已保存至: {shp_saved_path}\n"
            f"缓冲区边界框范围: ({minx}, {miny}) 与 ({maxx}, {maxy})之间\n"
            f"缓冲区查询成功: {buffer_query_result}"
        )

        return result


class ShpProcessorFactory:
    """文件操作器工厂"""

    OPERATION_PROCESSORS = {
        'convert': ConvertProcessor,
        'query': QueryProcessor,
        'buffer': BufferProcessor,
        'buffer_query': BufferQueryProcessor
    }

    @classmethod
    def create_processor(cls, source: Union[str, Dict], operation: str, params: dict):
        """创建处理器实例"""
        # 验证操作类型是否支持
        if operation not in cls.OPERATION_PROCESSORS:
            raise ValueError(f"不支持的操作类型: {operation}")

        # 获取对应的处理器类
        processor_class = cls.OPERATION_PROCESSORS[operation]

        # 创建处理器实例
        processor = processor_class(source, operation, params)

        # 执行处理逻辑
        process_result = processor.core()

        # 返回成功结果
        return success(process_result)
