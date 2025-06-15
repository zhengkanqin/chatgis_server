# GeoFile/Tools/ShpQueryTools.py
from typing import List

import geopandas as gpd

from GeoFile.Tools.TableExportTool import TableExporterFactory


class QueryProcessor:
    """属性查询处理器，支持多种查询模式并返回表格格式字符串"""

    def __init__(self, gdf, query_target, target_ids, attributes):
        self.gdf = gdf
        self.query_target = query_target
        self.target_ids = target_ids
        self.attributes = attributes

    def core(self) -> str:
        """
        执行SHP文件属性查询操作，返回表格格式字符串

        支持查询类型:
        - 独立查询(单要素字段): query_target='single'
        - 横向查询(单行): query_target='row'
        - 纵向查询(单列): query_target='column'
        - 全表查询: query_target='all'

        返回:
        - 表格格式的字符串 (包含表头和数据)
        """

        # 处理查询操作
        if self.query_target == 'single':
            return self._single_query(self.gdf, self.target_ids, self.attributes)
        elif self.query_target == 'row':
            return self._row_query(self.gdf, self.target_ids)
        elif self.query_target == 'column':
            return self._column_query(self.gdf, self.attributes)
        elif self.query_target == 'all':
            return self._all_query(self.gdf)
        else:
            raise ValueError(f"不支持的查询目标类型: {self.query_target}")

    @staticmethod
    def _single_query(gdf: gpd.GeoDataFrame, target_ids: List[int], attributes: List[str]) -> str:
        """
        独立查询 - 获取多个要素的指定字段值

        参数:
        - gdf: GeoDataFrame
        - target_ids: 目标要素ID列表
        - attributes: 要查询的属性字段列表

        返回:
        - 表格格式的字符串
        """
        # 验证输入
        if not target_ids:
            raise ValueError("独立查询需要提供target_id列表")
        if not attributes:
            raise ValueError("独立查询需要提供attributes列表")

        # 检查索引范围
        invalid_ids = [ids for ids in target_ids if ids < 0 or ids >= len(gdf)]
        if invalid_ids:
            raise ValueError(f"无效的要素ID: {invalid_ids}")

        # 检查属性是否存在
        missing_attrs = [attr for attr in attributes if attr not in gdf.columns]
        if missing_attrs:
            raise ValueError(f"缺少属性字段: {', '.join(missing_attrs)}")

        # 提取指定要素和属性
        subset = gdf.iloc[target_ids][attributes]

        # 转换为表格字符串
        return TableExporterFactory.export(subset, title="独立查询结果")

    @staticmethod
    def _row_query(gdf: gpd.GeoDataFrame, target_ids: List[int]) -> str:
        """
        横向查询 - 获取多个要素的所有字段值

        参数:
        - gdf: GeoDataFrame
        - target_ids: 目标要素ID列表

        返回:
        - 表格格式的字符串
        """
        # 验证输入
        if not target_ids:
            raise ValueError("横向查询需要提供target_id列表")

        # 检查索引范围
        invalid_ids = [ids for ids in target_ids if ids < 0 or ids >= len(gdf)]
        if invalid_ids:
            raise ValueError(f"无效的要素ID: {invalid_ids}")

        # 获取指定要素（排除几何列）
        attribute_columns = [col for col in gdf.columns]
        subset = gdf.iloc[target_ids][attribute_columns]

        # 转换为表格字符串
        return TableExporterFactory.export(subset, title="横向查询结果")

    @staticmethod
    def _column_query(gdf: gpd.GeoDataFrame, attributes: List[str]) -> str:
        """
        纵向查询 - 获取指定字段的所有值

        参数:
        - gdf: GeoDataFrame
        - attributes: 要查询的属性字段列表

        返回:
        - 表格格式的字符串
        """
        # 验证输入
        if not attributes:
            raise ValueError("纵向查询需要提供attributes列表")

        # 检查属性是否存在
        missing_attrs = [attr for attr in attributes if attr not in gdf.columns]
        if missing_attrs:
            raise ValueError(f"缺少属性字段: {', '.join(missing_attrs)}")

        # 提取指定列
        subset = gdf[attributes]

        # 转换为表格字符串
        return TableExporterFactory.export(subset, title="纵向查询结果")

    @staticmethod
    def _all_query(gdf: gpd.GeoDataFrame) -> str:
        """
        全表查询 - 获取整个属性表

        参数:
        - gdf: GeoDataFrame

        返回:
        - 表格格式的字符串
        """
        # 排除几何列
        attribute_columns = [col for col in gdf.columns]
        subset = gdf[attribute_columns]

        # 转换为表格字符串
        return TableExporterFactory.export(subset, title="全表查询结果")


def query_tool(gdf, query_target, target_ids, attributes):
    query_processor = QueryProcessor(gdf, query_target, target_ids, attributes)
    return query_processor.core()
