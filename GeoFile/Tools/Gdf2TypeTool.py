# GeoFile/Tools/Gdf2TypeTool.py
import os
from datetime import datetime
from typing import Optional, List, Type, Tuple, Any

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_origin


class BaseGdfConverter:
    """
    GeoDataFrame转换器的基类，提供通用功能

    参数:
    - base_name: 输出文件的基础名称
    - gdf: 包含地理数据的GeoDataFrame
    - attributes: 需要处理的属性字段列表
    - custom_output_path: 自定义输出路径
    """

    # 支持的输出格式及其扩展名
    SUPPORTED_FORMATS = {
        'geojson': '.geojson',
        'png': '.png',
        'shp': '.shp',
        'gpkg': '.gpkg',
        'kml': '.kml',
        'geotiff': '.tif'
    }

    def __init__(self,
                 gdf: gpd.GeoDataFrame,
                 type_name: str,
                 attributes: Optional[List[str]] = None,
                 custom_output_path: Optional[str] = None,
                 **kwargs):
        self.gdf = gdf
        self.type_name = type_name
        self.attributes = attributes or []
        self.custom_output_path = custom_output_path
        self.kwargs = kwargs  # 其他可选参数

        # 验证属性字段是否存在
        self._validate_attributes()

        # 自动生成基础名称
        self.base_name = self._generate_base_name()

    def _validate_attributes(self):
        """验证请求的属性字段是否存在"""
        if self.attributes:
            required_columns = set(self.attributes) | {'geometry'}
            missing_columns = required_columns - set(self.gdf.columns)
            if missing_columns:
                raise ValueError(
                    f"请求的属性字段不存在: {', '.join(missing_columns)}"
                )

    def _generate_base_name(self) -> str:
        """自动生成基础文件名"""
        # 尝试从GeoDataFrame属性中获取原始文件名
        if hasattr(self.gdf, 'attrs') and 'source_name' in self.gdf.attrs:
            return self.gdf.attrs['source_name']

        # 创建描述性名称
        return f"{self.type_name}"

    def _prepare_output_dir(self, default_subdir: str) -> str:
        """准备输出目录"""
        if self.custom_output_path:
            output_dir = os.path.abspath(self.custom_output_path)
        else:
            # 创建默认结果目录
            output_dir = os.path.abspath(f"GeoFile/Result/{default_subdir}")

        os.makedirs(output_dir, exist_ok=True)
        return output_dir

    def _generate_output_filename(self, extension: str) -> str:
        """生成带时间戳的唯一输出文件名"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{self.base_name}_{timestamp}.{extension}"

    def convert(self) -> Any:
        """执行转换操作，子类必须实现此方法"""
        raise NotImplementedError("子类必须实现convert方法")

    def result(self) -> str:
        """执行输出操作，子类必须实现此方法"""
        raise NotImplementedError("子类必须实现result方法")

    def get_bbox(self) -> Tuple[float, float, float, float]:
        """获取地理范围边界框"""
        minx, miny, maxx, maxy = self.gdf.total_bounds
        return minx - 0.01, miny - 0.01, maxx + 0.01, maxy + 0.01


class GdfToGeoJSONConverter(BaseGdfConverter):
    """将GeoDataFrame转换为GeoJSON"""

    def __init__(self,
                 gdf: gpd.GeoDataFrame,
                 type_name: str,
                 attributes: Optional[List[str]] = None,
                 custom_output_path: Optional[str] = None,
                 **kwargs):
        super().__init__(gdf, type_name, attributes, custom_output_path)
        self.attribute_count = None
        self.feature_count = None
        self.output_path = None

    def convert(self):
        """执行GeoJSON转换"""
        # 筛选数据
        if self.attributes:
            required_columns = set(self.attributes) | {'geometry'}
            gdf = self.gdf[list(required_columns)]
        else:
            gdf = self.gdf

        # 统计信息
        feature_count = len(gdf)
        attribute_count = len(gdf.columns) - 1  # 减去几何字段

        # 准备输出目录
        output_dir = self._prepare_output_dir("GeoJSONs")
        output_file = self._generate_output_filename("geojson")
        output_path = os.path.join(output_dir, output_file)

        # 保存为GeoJSON文件
        gdf.to_file(output_path, driver="GeoJSON")

        self.output_path = output_path
        self.feature_count = feature_count
        self.attribute_count = attribute_count

        return output_path, feature_count, attribute_count

    def result(self) -> str:
        return (
            f"GeoJSON文件已保存至: {self.output_path}\n"
            f"要素数量: {self.feature_count}\n"
            f"属性字段数量: {self.attribute_count}"
        )


class GdfToShapefileConverter(BaseGdfConverter):
    """将GeoDataFrame转换为Shapefile"""

    def __init__(self,
                 gdf: gpd.GeoDataFrame,
                 type_name: str,
                 attributes: Optional[List[str]] = None,
                 custom_output_path: Optional[str] = None,
                 **kwargs):
        super().__init__(gdf, type_name, attributes, custom_output_path)
        self.file_count = None
        self.attribute_count = None
        self.feature_count = None
        self.output_path = None

    def convert(self):
        """执行Shapefile转换"""
        # 筛选数据
        if self.attributes:
            required_columns = set(self.attributes) | {'geometry'}
            gdf = self.gdf[list(required_columns)]
        else:
            gdf = self.gdf

        # 统计信息
        feature_count = len(gdf)
        attribute_count = len(gdf.columns) - 1  # 减去几何字段

        # 准备输出目录
        output_dir = self._prepare_output_dir("Shapefiles")
        output_file = self._generate_output_filename(".shp")
        output_path = os.path.join(output_dir, output_file)

        # 保存为Shapefile
        gdf.to_file(output_path, driver="ESRI Shapefile")

        # 统计生成的文件数量
        file_count = len([f for f in os.listdir(output_dir) if f.startswith(os.path.basename(output_file)[:-4])])

        self.output_path = output_path
        self.feature_count = feature_count
        self.attribute_count = attribute_count
        self.file_count = file_count

        return output_path, feature_count, attribute_count, file_count

    def result(self) -> str:
        return (
            f"Shapefile主文件已保存至: {self.output_path}\n"
            f"要素数量: {self.feature_count}\n"
            f"属性字段数量: {self.attribute_count}\n"
            f"生成文件总数: {self.file_count}"
        )


class GdfToGeoPackageConverter(BaseGdfConverter):
    """将GeoDataFrame转换为GeoPackage"""

    def __init__(self,
                 gdf: gpd.GeoDataFrame,
                 type_name: str,
                 attributes: Optional[List[str]] = None,
                 custom_output_path: Optional[str] = None,
                 **kwargs):
        super().__init__(gdf, type_name, attributes, custom_output_path)
        self.attribute_count = None
        self.feature_count = None
        self.output_path = None

    def convert(self):
        """执行GeoPackage转换"""
        # 筛选数据
        if self.attributes:
            required_columns = set(self.attributes) | {'geometry'}
            gdf = self.gdf[list(required_columns)]
        else:
            gdf = self.gdf

        # 统计信息
        feature_count = len(gdf)
        attribute_count = len(gdf.columns) - 1  # 减去几何字段

        # 准备输出目录
        output_dir = self._prepare_output_dir("GeoPackages")
        output_file = self._generate_output_filename(".gpkg")
        output_path = os.path.join(output_dir, output_file)

        # 保存为GeoPackage
        gdf.to_file(output_path, driver="GPKG")

        self.output_path = output_path
        self.feature_count = feature_count
        self.attribute_count = attribute_count

        return output_path, feature_count, attribute_count

    def result(self) -> str:
        return (
            f"GeoPackage文件已保存至: {self.output_path}\n"
            f"要素数量: {self.feature_count}\n"
            f"属性字段数量: {self.attribute_count}"
        )


class GdfToKMLConverter(BaseGdfConverter):
    """将GeoDataFrame转换为KML"""

    def __init__(self,
                 gdf: gpd.GeoDataFrame,
                 type_name: str,
                 attributes: Optional[List[str]] = None,
                 custom_output_path: Optional[str] = None,
                 **kwargs):
        super().__init__(gdf, type_name, attributes, custom_output_path)
        self.attribute_count = None
        self.feature_count = None
        self.output_path = None

    def convert(self):
        """执行KML转换"""
        # 筛选数据
        if self.attributes:
            required_columns = set(self.attributes) | {'geometry'}
            gdf = self.gdf[list(required_columns)]
        else:
            gdf = self.gdf

        # 统计信息
        feature_count = len(gdf)
        attribute_count = len(gdf.columns) - 1  # 减去几何字段

        # 准备输出目录
        output_dir = self._prepare_output_dir("KMLs")
        output_file = self._generate_output_filename(".kml")
        output_path = os.path.join(output_dir, output_file)

        # 保存为KML
        gdf.to_file(output_path, driver="KML")

        self.output_path = output_path
        self.feature_count = feature_count
        self.attribute_count = attribute_count

        return output_path, feature_count, attribute_count

    def result(self) -> str:
        return (
            f"KML文件已保存至: {self.output_path}\n"
            f"要素数量: {self.feature_count}\n"
            f"属性字段数量: {self.attribute_count}"
        )


class GdfToGeoTIFFConverter(BaseGdfConverter):
    """将GeoDataFrame转换为GeoTIFF"""

    def __init__(self,
                 gdf: gpd.GeoDataFrame,
                 type_name: str,
                 attributes: Optional[List[str]] = None,
                 custom_output_path: Optional[str] = None,
                 **kwargs):
        super().__init__(gdf, type_name, attributes, custom_output_path)
        self.attribute = None
        self.output_path = None
        self.resolution = None

    def convert(self):
        """执行GeoTIFF转换"""
        # 获取参数
        resolution = self.kwargs.get('resolution', 10)  # 默认分辨率10米/像素
        attribute = self.kwargs.get(self.attributes[0], None)  # 用于栅格化的属性
        nodata = self.kwargs.get('nodata', -9999)  # 无数据值

        # 获取地理范围
        minx, miny, maxx, maxy = self.get_bbox()

        # 计算栅格尺寸
        width = int((maxx - minx) / resolution)
        height = int((maxy - miny) / resolution)

        # 创建转换矩阵
        transform = from_origin(minx, maxy, resolution, resolution)

        # 准备输出目录
        output_dir = self._prepare_output_dir("GeoTIFFs")
        output_file = self._generate_output_filename(".tif")
        output_path = os.path.join(output_dir, output_file)

        # 栅格化数据
        if attribute and attribute in self.gdf.columns:
            # 根据属性值栅格化
            shapes = [(geom, value) for geom, value in zip(self.gdf.geometry, self.gdf[attribute])]
            raster = rasterize(
                shapes,
                out_shape=(height, width),
                transform=transform,
                fill=nodata,
                dtype=self.gdf[attribute].dtype
            )
        else:
            # 二进制栅格化（存在/不存在）
            shapes = [(geom, 1) for geom in self.gdf.geometry]
            raster = rasterize(
                shapes,
                out_shape=(height, width),
                transform=transform,
                fill=0,
                dtype=np.uint8
            )

        # 保存GeoTIFF
        with rasterio.open(
                output_path, 'w',
                driver='GTiff',
                height=height,
                width=width,
                count=1,
                dtype=raster.dtype,
                crs=self.gdf.crs,
                transform=transform,
                nodata=nodata
        ) as dst:
            dst.write(raster, 1)

        self.output_path = output_path
        self.resolution = resolution
        self.attribute = attribute

        return output_path, resolution, attribute

    def result(self) -> str:
        if self.attribute:
            return (
                f"GeoTIFF文件已保存至: {self.output_path}\n"
                f"分辨率: {self.resolution} 米/像素\n"
                f"栅格化属性: {self.attribute}"
            )
        else:
            return (
                f"GeoTIFF文件已保存至: {self.output_path}\n"
                f"分辨率: {self.resolution} 米/像素\n"
                f"栅格化类型: 二进制（存在/不存在）"
            )


class GdfToPNGConverter(BaseGdfConverter):
    """将GeoDataFrame转换为PNG图像"""

    def __init__(self,
                 gdf: gpd.GeoDataFrame,
                 type_name: str,
                 attributes: Optional[List[str]] = None,
                 custom_output_path: Optional[str] = None,
                 **kwargs):
        super().__init__(gdf, type_name, attributes, custom_output_path)
        self.png_path = None
        self.bbox = None

    def convert(self):
        """执行PNG转换"""
        # 获取地理范围
        bbox = self.get_bbox()

        # 创建绘图 - 设置透明背景
        fig, ax = plt.subplots(figsize=(10, 10), facecolor='none')
        ax.set_aspect('equal')
        fig.patch.set_alpha(0)  # 完全透明背景

        # 移除所有坐标轴和边框
        ax.axis('off')
        ax.set_frame_on(False)

        # 根据属性设置样式
        if self.attributes and len(self.attributes) > 0:
            self._plot_with_attributes(ax)
        else:
            # 无属性时使用统一颜色
            self.gdf.plot(ax=ax, color='blue', edgecolor='black')

        # 设置坐标范围
        ax.set_xlim(bbox[0], bbox[2])
        ax.set_ylim(bbox[1], bbox[3])

        # 准备输出目录
        output_dir = self._prepare_output_dir("Images")
        output_file = self._generate_output_filename("png")
        png_path = os.path.join(output_dir, output_file)

        # 保存为PNG
        plt.savefig(png_path, dpi=600, bbox_inches='tight')
        plt.close(fig)

        self.png_path = png_path
        self.bbox = bbox

        return png_path, bbox

    def _plot_with_attributes(self, ax):
        """根据属性值绘制不同颜色"""
        # 选择一个属性用于分类着色
        color_by = self.attributes[0]

        # 数值型属性使用渐变着色
        if self.gdf[color_by].dtype.kind in 'ifc':
            self.gdf.plot(column=color_by, ax=ax, legend=True,
                          cmap='viridis', legend_kwds={'shrink': 0.5})
        # 分类属性使用离散着色
        else:
            unique_values = self.gdf[color_by].unique()
            num_colors = len(unique_values)

            # 获取合适的颜色映射
            if num_colors <= 10:
                cmap = plt.get_cmap('tab10')
            elif num_colors <= 20:
                cmap = plt.get_cmap('tab20')
            else:
                # 对于超过20种类别，使用连续颜色映射
                cmap = plt.get_cmap('viridis')

            # 生成颜色
            colors = cmap(np.linspace(0, 1, num_colors))

            # 绘制每个类别
            for value, color in zip(unique_values, colors):
                subset = self.gdf[self.gdf[color_by] == value]
                if not subset.empty:
                    subset.plot(
                        ax=ax,
                        color=color,
                        label=str(value),  # 确保值为字符串
                        edgecolor='none'  # 可选：移除边界线
                    )

    def result(self):
        # 解构边界框坐标
        minx, miny, maxx, maxy = self.bbox

        return (
            f"PNG文件已保存至: {self.png_path}\n"
            f"边界框范围: ({minx}, {miny}) 与 ({maxx}, {maxy})之间"
        )


class ConverterFactory:
    """转换器工厂类，根据类型名称创建对应的转换器"""

    # 注册的转换器映射
    CONVERTER_REGISTRY = {
        'geojson': GdfToGeoJSONConverter,
        'png': GdfToPNGConverter,
        'shp': GdfToShapefileConverter,
        'gpkg': GdfToGeoPackageConverter,
        'kml': GdfToKMLConverter,
        'geotiff': GdfToGeoTIFFConverter
    }

    @classmethod
    def get_converter(cls,
                      type_name: str,
                      gdf: gpd.GeoDataFrame,
                      attributes: Optional[List[str]] = None,
                      custom_output_path: Optional[str] = None) -> BaseGdfConverter:
        """
        获取指定类型的转换器

        参数:
        - type_name: 转换类型名称 (如 'geojson', 'png')
        - base_name: 输出文件的基础名称
        - gdf: 包含地理数据的GeoDataFrame
        - attributes: 需要处理的属性字段列表
        - custom_output_path: 自定义输出路径

        返回:
        - 对应的转换器实例
        """
        converter_class = cls.CONVERTER_REGISTRY.get(type_name.lower())
        if not converter_class:
            raise ValueError(f"暂不支持转换的文件格式: {type_name}")

        return converter_class(
            gdf=gdf,
            type_name=type_name,
            attributes=attributes,
            custom_output_path=custom_output_path
        )

    @classmethod
    def register_converter(cls, type_name: str, converter_class: Type[BaseGdfConverter]):
        """
        注册新的转换器类型

        参数:
        - type_name: 转换类型名称
        - converter_class: 转换器类（必须是BaseShpConverter的子类）
        """
        if not issubclass(converter_class, BaseGdfConverter):
            raise TypeError("转换器必须是BaseShpConverter的子类")

        cls.CONVERTER_REGISTRY[type_name.lower()] = converter_class
