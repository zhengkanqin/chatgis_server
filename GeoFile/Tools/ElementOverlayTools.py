# GeoFile/Tools/ElementOverlayTools.py
import os
from typing import Dict, Optional, Type

import geopandas as gpd
import pandas as pd

from GeoFile.Tools.Gdf2TypeTool import ConverterFactory


class BaseElementOverlay:
    """要素叠加分析基类"""

    def __init__(self,
                 main_gdf: gpd.GeoDataFrame,
                 overlay_gdf: gpd.GeoDataFrame,
                 join_attribute: str = "ALL",
                 tolerance: Optional[float] = None):
        """
        初始化叠加分析器

        参数:
        main_gdf: 主输入地理数据框
        overlay_gdf: 叠加层地理数据框
        join_attribute: 属性传递规则 ("ALL", "NO_FID", "ONLY_FID")
        tolerance: 坐标容差值
        """
        self.main_gdf = main_gdf
        self.overlay_gdf = overlay_gdf
        self.join_attribute = join_attribute
        self.tolerance = tolerance
        self.result_gdf = None

    def _validate_geometries(self):
        """验证几何类型是否符合操作要求"""
        raise NotImplementedError("子类必须实现此方法")

    def _perform_overlay(self):
        """执行具体的叠加操作"""
        raise NotImplementedError("子类必须实现此方法")

    def _handle_attributes(self):
        """根据join_attribute参数处理属性字段"""
        if self.join_attribute == "ALL":
            return  # 保留所有属性

        fid_columns = [col for col in self.result_gdf.columns
                       if col.lower() == 'fid' or col.endswith('_fid')]

        if self.join_attribute == "NO_FID":
            # 删除所有FID相关字段
            self.result_gdf = self.result_gdf.drop(columns=fid_columns)

        elif self.join_attribute == "ONLY_FID":
            # 只保留FID字段
            keep_cols = ['geometry'] + fid_columns
            self.result_gdf = self.result_gdf[keep_cols]

    def execute(self) -> gpd.GeoDataFrame:
        """执行叠加分析并返回结果"""
        self._validate_geometries()
        self._perform_overlay()
        self._handle_attributes()
        return self.result_gdf


class IdentityOverlay(BaseElementOverlay):
    """标识叠加分析"""

    def _validate_geometries(self):
        main_type = self.main_gdf.geometry.type.iloc[0]
        overlay_type = self.overlay_gdf.geometry.type.iloc[0]

        if overlay_type != "Polygon" and main_type != overlay_type:
            raise ValueError("标识操作要求叠加层为面或与输入要素相同几何类型")

    def _perform_overlay(self):
        self.result_gdf = gpd.overlay(
            self.main_gdf,
            self.overlay_gdf,
            how='identity',
            keep_geom_type=True
        )


class EraseOverlay(BaseElementOverlay):
    """擦除叠加分析"""

    def _validate_geometries(self):
        if self.overlay_gdf.geometry.type.iloc[0] != "Polygon":
            raise ValueError("擦除操作要求叠加层为面要素")

    def _perform_overlay(self):
        self.result_gdf = gpd.overlay(
            self.main_gdf,
            self.overlay_gdf,
            how='difference',
            keep_geom_type=True
        )


class UpdateOverlay(BaseElementOverlay):
    """更新叠加分析"""

    def _validate_geometries(self):
        if (self.main_gdf.geometry.type.iloc[0] != "Polygon" or
                self.overlay_gdf.geometry.type.iloc[0] != "Polygon"):
            raise ValueError("更新操作要求主数据和叠加层均为面要素")

    def _perform_overlay(self):
        # 1. 擦除重叠部分
        erased = gpd.overlay(
            self.main_gdf,
            self.overlay_gdf,
            how='difference',
            keep_geom_type=True
        )

        # 2. 合并更新要素
        self.result_gdf = gpd.GeoDataFrame(
            pd.concat([erased, self.overlay_gdf], ignore_index=True),
            crs=self.main_gdf.crs
        )


class SymDifferenceOverlay(BaseElementOverlay):
    """对称差异叠加分析"""

    def _validate_geometries(self):
        main_type = self.main_gdf.geometry.type.iloc[0]
        overlay_type = self.overlay_gdf.geometry.type.iloc[0]

        if main_type != overlay_type:
            raise ValueError("对称差异操作要求主数据和叠加层具有相同几何类型")

    def _perform_overlay(self):
        self.result_gdf = gpd.overlay(
            self.main_gdf,
            self.overlay_gdf,
            how='symmetric_difference',
            keep_geom_type=True
        )


class SpatialJoinOverlay(BaseElementOverlay):
    """空间连接叠加分析"""

    def _validate_geometries(self):
        # 空间连接支持不同类型几何
        pass

    def _perform_overlay(self):
        self.result_gdf = gpd.sjoin(
            self.main_gdf,
            self.overlay_gdf,
            how='inner',
            predicate='intersects'
        )


class UnionOverlay(BaseElementOverlay):
    """联合叠加分析"""

    def _validate_geometries(self):
        if self.overlay_gdf.geometry.type.iloc[0] != "Polygon":
            raise ValueError("联合操作要求叠加层为面要素")

    def _perform_overlay(self):
        self.result_gdf = gpd.overlay(
            self.main_gdf,
            self.overlay_gdf,
            how='union',
            keep_geom_type=True
        )


class IntersectOverlay(BaseElementOverlay):
    """相交叠加分析"""

    def _validate_geometries(self):
        main_type = self.main_gdf.geometry.type.iloc[0]
        overlay_type = self.overlay_gdf.geometry.type.iloc[0]

        if main_type != overlay_type:
            raise ValueError("相交操作要求主数据和叠加层具有相同几何类型")

    def _perform_overlay(self):
        self.result_gdf = gpd.overlay(
            self.main_gdf,
            self.overlay_gdf,
            how='intersection',
            keep_geom_type=True
        )


class ElementOverlayFactory:
    """要素叠加分析工厂类"""

    OVERLAY_REGISTRY = {
        'identity': IdentityOverlay,
        'erase': EraseOverlay,
        'update': UpdateOverlay,
        'symdifference': SymDifferenceOverlay,
        'spatial_join': SpatialJoinOverlay,
        'union': UnionOverlay,
        'intersect': IntersectOverlay,
    }

    @classmethod
    def get_element_overlay(cls,
                            operation: str,
                            main_gdf: gpd.GeoDataFrame,
                            overlay_gdf: gpd.GeoDataFrame,
                            **kwargs) -> BaseElementOverlay:
        """
        获取指定操作的叠加分析器

        参数:
        operation: 叠加操作类型
        main_gdf: 主输入地理数据框
        overlay_gdf: 叠加层地理数据框
        kwargs: 其他参数 (join_attribute, tolerance)

        返回:
        对应的叠加分析器实例
        """
        overlay_class = cls.OVERLAY_REGISTRY.get(operation.lower())
        if not overlay_class:
            raise ValueError(f"暂不支持的叠加操作类型: {operation}")

        return overlay_class(main_gdf, overlay_gdf, **kwargs)

    @classmethod
    def register_overlay(cls, operation: str, overlay_class: Type[BaseElementOverlay]):
        """注册新的叠加操作类型"""
        if not issubclass(overlay_class, BaseElementOverlay):
            raise TypeError("叠加分析器必须是BaseElementOverlay的子类")
        cls.OVERLAY_REGISTRY[operation.lower()] = overlay_class


class ElementOverlayResultProcessor:
    """叠加分析结果处理器"""

    def __init__(self, result_gdf: gpd.GeoDataFrame, operation: str):
        """
        初始化结果处理器

        参数:
        result_gdf: 叠加分析结果地理数据框
        operation: 执行的叠加操作类型
        """
        self.result_gdf = result_gdf
        self.operation = operation

    def generate_outputs(self) -> Dict:
        """生成所有输出结果"""
        # 创建输出目录
        output_dir = os.path.abspath("GeoFile/Result/_overlay_result")
        os.makedirs(output_dir, exist_ok=True)

        # 生成GeoJSON
        geojson_path = self._generate_geojson(output_dir)

        # 生成Shapefile
        shp_path = self._generate_shapefile(output_dir)

        # 生成统计信息
        stats_str = self._generate_stats_string(geojson_path, shp_path)

        return {
            "geojson_path": geojson_path,
            "shp_path": shp_path,
            "stats": stats_str,
            "feature_count": len(self.result_gdf)
        }

    def _generate_geojson(self, output_dir: str) -> str:
        """生成GeoJSON文件"""
        converter = ConverterFactory.get_converter(
            type_name='geojson',
            gdf=self.result_gdf,
            custom_output_path=output_dir
        )
        geojson_path, _, _ = converter.convert()
        return geojson_path

    def _generate_shapefile(self, output_dir: str) -> str:
        """生成Shapefile文件"""
        converter = ConverterFactory.get_converter(
            type_name='shp',
            gdf=self.result_gdf,
            custom_output_path=output_dir
        )
        shp_path, _, _, _ = converter.convert()
        return shp_path

    def _generate_stats_string(self, geojson_path: str, shp_path: str) -> str:
        """生成结果统计字符串"""
        operation_name = self.operation.capitalize()
        result = f"{operation_name}操作结果文件已生成：\n"
        result += f"  - GeoJSON: {geojson_path}\n"
        result += f"  - Shapefile: {shp_path}\n"
        result += f"输出要素数量: {len(self.result_gdf)}\n"

        return result
