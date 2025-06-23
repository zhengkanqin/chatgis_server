# GeoFile/Processors/SpatialOperationProcessor.py
"""
空间分析操作处理模块

支持空间查询等功能。
"""
import os
from abc import ABC, abstractmethod
from typing import Dict, Any, Union

import geopandas as gpd

from GeoFile.Common.Message import success
from GeoFile.Tools.ClusterAnalysisTool import ClusterFactory, ClusterResultProcessor
from GeoFile.Tools.ElementOverlayTools import ElementOverlayFactory
from GeoFile.Tools.Gdf2TypeTool import ConverterFactory
from GeoFile.Tools.GeographicObjectTool import read_geographic_data
from GeoFile.Tools.ProximityTools import ProximityResultProcessor, ProximityFactory
from GeoFile.Tools.SpatialQueryTool import spatial_query_tool
from GeoFile.Tools.VoronoiPolygonTool import create_voronoi_polygons


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
    def core(self):
        """处理入口方法（需子类实现）"""
        pass


class SpatialQueryProcessor(BaseOperationProcessor):
    """空间查询处理器"""

    SUPPORTED_OPERATION = ['spatial_query']

    def core(self):
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


class ClusterAnalysisProcessor(BaseOperationProcessor):
    """空间聚类分析处理器"""

    SUPPORTED_OPERATION = ['cluster_analysis']

    def core(self):
        gdf = self.gdf
        algorithm = self.params.get('algorithm')
        use_attributes = self.params.get("attributes", [])
        n_clusters = self.params.get('n_clusters', None)
        eps = self.params.get('eps', None)

        # 准备算法特定参数
        algorithm_params = {}
        if n_clusters is not None:
            algorithm_params['n_clusters'] = n_clusters
        if eps is not None:
            algorithm_params['eps'] = eps

        # 使用工厂创建聚类器
        cluster = ClusterFactory.get_cluster(
            algorithm=algorithm,
            gdf=gdf,
            use_attributes=use_attributes,
            **algorithm_params  # 只传递算法特定参数
        )

        # 执行聚类
        clustered_gdf = cluster.fit_predict()
        cluster_stats = cluster.get_cluster_stats()

        # 添加算法参数到统计信息
        cluster_stats['params'] = algorithm_params

        # 处理结果并生成输出
        processor = ClusterResultProcessor(clustered_gdf, cluster_stats)
        results = processor.generate_outputs()

        return results.get("stats")


class ElementOverlayProcessor(BaseOperationProcessor):
    """要素叠加分析处理器"""

    SUPPORTED_OPERATION = ['element_overlay']

    def core(self):
        gdf = self.gdf
        operation = self.params.get('operation')
        overlay_source = self.params.get('overlay_source')
        overlay_gdf = read_geographic_data(overlay_source)
        join_attribute = self.params.get('join_attribute')
        tolerance = self.params.get("tolerance", None)

        # 准备算法特定参数
        overlay_params = {'join_attribute': join_attribute}
        if tolerance is not None:
            overlay_params['tolerance'] = tolerance

        overlay = ElementOverlayFactory.get_element_overlay(
            operation=operation,
            main_gdf=gdf,
            overlay_gdf=overlay_gdf,
            **overlay_params
        )

        result_gdf = overlay.execute()

        # 处理结果并生成输出
        processor = ClusterResultProcessor(result_gdf, operation)
        results = processor.generate_outputs()

        return results.get("stats")


class ThiessenPolygonProcessor(BaseOperationProcessor):
    """泰森多边形生成器"""

    SUPPORTED_OPERATION = ['thiessen_polygon']

    def core(self):
        gdf = self.gdf

        result_gdf = create_voronoi_polygons(gdf)

        # 创建目录存储输出文件
        output_dir = os.path.abspath(f"GeoFile/Result/_voronoi")
        os.makedirs(output_dir, exist_ok=True)

        # 生成GeoJSON
        converter = ConverterFactory.get_converter(
            type_name='geojson',
            gdf=result_gdf,
            custom_output_path=output_dir
        )
        geojson_path, _, _ = converter.convert()

        # 生成Shapefile
        converter = ConverterFactory.get_converter(
            type_name='shp',
            gdf=result_gdf,
            custom_output_path=output_dir
        )
        shp_path, _, _, _ = converter.convert()

        # 格式化结果字符串
        result = (
            f"泰森多边形生成成功！\n"
            f"多边形GeoJSON文件已保存至: {geojson_path}\n"
            f"多边形SHP文件已保存至: {shp_path}\n"
        )

        return result


class ProximityProcessor(BaseOperationProcessor):
    """近邻分析处理器"""

    SUPPORTED_OPERATION = ['proximity_analysis']

    def core(self):
        main_gdf = self.gdf

        # 读取邻近要素数据（如果提供）
        near_gdf = read_geographic_data(self.params.get("near_source")) if self.params.get("near_source") else None
        max_distance = self.params.get("max_distance")
        include_attributes = self.params.get("include_attributes")
        buffer_distance = self.params.get("buffer_distance")

        # 准备算法特定参数
        proximity_params = {}
        if max_distance is not None:
            proximity_params['max_distance'] = max_distance
        if include_attributes is not None:
            proximity_params['include_attributes'] = include_attributes
        if buffer_distance is not None:
            proximity_params['buffer_distance'] = buffer_distance

        # 获取邻近分析工具
        tool = ProximityFactory.get_proximity_tool(
            tool_name=self.params.get("tool_name"),
            main_gdf=main_gdf,
            near_gdf=near_gdf,
            **proximity_params
        )

        # 执行分析
        result = tool.execute()

        # 获取统计信息
        stats = tool.get_result_stats()

        # 处理结果
        processor = ProximityResultProcessor(result, stats, self.params.get("tool_name"))
        outputs = processor.generate_outputs()

        return outputs.get("stats")


class SpatialProcessorFactory:
    """文件操作器工厂"""

    OPERATION_PROCESSORS = {
        'spatial_query': SpatialQueryProcessor,
        'cluster_analysis': ClusterAnalysisProcessor,
        'element_overlay': ElementOverlayProcessor,
        'thiessen_polygon': ThiessenPolygonProcessor,
        'proximity_analysis': ProximityProcessor
    }

    @classmethod
    def create_processor(cls, operation: str, source: Union[str, Dict], params: dict):
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
        process_result = processor.core()

        # 返回成功结果
        return success(process_result)
