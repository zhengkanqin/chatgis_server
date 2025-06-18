# GeoFile/Processors/SpatialOperationProcessor.py
"""
空间分析操作处理模块

支持空间查询等功能。
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Union

import geopandas as gpd

from GeoFile.Common.Message import success
from GeoFile.Tools.GeographicObjectTool import read_geographic_data
from GeoFile.Tools.SpatialQueryTool import spatial_query_tool


class BaseOperationProcessor(ABC):
    """文件操作器基类"""

    SUPPORTED_OPERATION = []

    def __init__(self, operation: str, gdf: gpd.GeoDataFrame, params: Dict[str, Any]):
        """
        :param operation: 操作类型
        :param gdf: 核心操作对象
        """
        self.operation = operation
        self.gdf = gdf
        self.params = params
        self._validate()
        self._extract()

    def _validate(self):
        """基础验证"""
        if self.operation not in self.SUPPORTED_OPERATION:
            raise ValueError(f"操作类型'{self.operation}'不被支持")

    def _extract(self):
        pass

    @abstractmethod
    async def core(self):
        """处理入口方法（需子类实现）"""
        pass


class SpatialQueryProcessor(BaseOperationProcessor):
    """空间查询处理器"""

    SUPPORTED_OPERATION = ['spatial_query']

    async def core(self):
        origin_gdf = self.gdf
        query_source = self.params.get('query_source')
        query_gdf = read_geographic_data(query_source)
        relation = self.params.get('relation')

        result_dict = spatial_query_tool(origin_gdf, query_gdf, relation=relation)

        # 格式化结果字符串
        result = (
            f"共找到 {result_dict.get("result_num")} 个相交要素\n"
            f"空间查询结果SHP文件已保存至: {result_dict.get("result_shp_path")}\n"
            f"空间查询结果GeoJSON文件已保存至: {result_dict.get("result_geojson_path")}\n"
        )

        return result


class SpatialProcessorFactory:
    """文件操作器工厂"""

    OPERATION_PROCESSORS = {
        'spatial_query': SpatialQueryProcessor
    }

    @classmethod
    async def create_processor(cls, operation: str, source: Union[str, Dict], params: dict):
        """创建处理器实例"""
        # 验证操作类型是否支持
        if operation not in cls.OPERATION_PROCESSORS:
            raise ValueError(f"不支持的操作类型: {operation}")

        queried_condition = params.get('query_condition')
        gdf = read_geographic_data(source, queried_condition)

        # 获取对应的处理器类
        processor_class = cls.OPERATION_PROCESSORS[operation]

        # 创建处理器实例
        processor = processor_class(operation, gdf, params)

        # 执行处理逻辑
        process_result = await processor.core()

        # 返回成功结果
        return await success(process_result)
