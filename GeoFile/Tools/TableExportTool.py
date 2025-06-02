# GeoFile/Tools/TableExportTool.py
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from geopandas import GeoDataFrame
from pandas import DataFrame


class BaseExporter(ABC):
    """
    导出器的抽象基类，定义统一的导出接口
    """

    @abstractmethod
    def export(self, gdf: GeoDataFrame, output_path: str = None, title: str = "") -> str:
        """
        将GeoDataFrame导出为指定格式

        参数:
        - gdf: 要导出的GeoDataFrame
        - output_path: 输出文件路径（对于str导出器可选）
        - title: 表格标题（仅对文本格式有效）

        返回:
        - 文件路径（文件导出器）或字符串内容（str导出器）
        """
        pass

    @staticmethod
    def _to_table_string(df: DataFrame, title: str = "") -> str:
        """
        将DataFrame转换为表格格式的字符串

        参数:
        - df: 要转换的DataFrame
        - title: 表格标题
        """
        table_str = ""
        if title:
            table_str += f"{title}\n"
        table_str += str(df)
        return table_str

    @staticmethod
    def output_dir(output_path: str) -> str:
        if output_path:
            # 使用自定义的完整输出路径
            output_dir = os.path.abspath(output_path)
            # 确保目录存在
            if output_dir:  # 如果路径包含目录
                os.makedirs(output_dir, exist_ok=True)
        else:
            # 创建默认结果目录
            output_dir = os.path.abspath("GeoFile/Result")
            os.makedirs(output_dir, exist_ok=True)

        # 生成唯一的输出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        return os.path.join(output_dir, f"output_{timestamp}.xlsx")


class ExcelExporter(BaseExporter):
    """导出为Excel格式 (.xlsx)"""

    def export(self, gdf: GeoDataFrame, title: str = "", output_path: Optional[str] = None) -> str:
        output_path = self.output_dir(output_path)

        # 将几何列转换为WKT文本以便在Excel中显示
        temp_df = gdf.copy()
        if 'geometry' in temp_df.columns:
            temp_df['geometry'] = temp_df['geometry'].apply(lambda geom: geom.wkt)

        temp_df.to_excel(output_path, index=False)
        return os.path.abspath(output_path)


class CsvExporter(BaseExporter):
    """导出为CSV格式 (.csv)"""

    def export(self, gdf: GeoDataFrame, title: str = "", output_path: Optional[str] = None) -> str:
        output_path = self.output_dir(output_path)

        # 将几何列转换为WKT文本
        temp_df = gdf.copy()
        if 'geometry' in temp_df.columns:
            temp_df['geometry'] = temp_df['geometry'].apply(lambda geom: geom.wkt)

        temp_df.to_csv(output_path, index=False)
        return os.path.abspath(output_path)


class TxtExporter(BaseExporter):
    """导出为文本表格格式 (.txt)"""

    def export(self, gdf: GeoDataFrame, title: str = "", output_path: Optional[str] = None) -> str:
        output_path = self.output_dir(output_path)

        # 使用基类的表格字符串转换方法
        table_str = self._to_table_string(gdf, title)

        with open(output_path, 'w') as f:
            f.write(table_str)
        return os.path.abspath(output_path)


class StrExporter(BaseExporter):
    """导出为字符串格式"""

    def export(self, gdf: GeoDataFrame, title: str = "", output_path: Optional[str] = None) -> str:
        # 忽略output_path参数，直接返回字符串
        return self._to_table_string(gdf, title)


class TableExporterFactory:
    """
    导出器工厂类，根据格式类型创建相应的导出器
    """

    @staticmethod
    def export(gdf, title: str = "", format_type: Optional[str] = 'str') -> str:
        """
        获取指定格式的导出器实例

        参数:
        - format_type: 导出格式 ('excel', 'txt', 'csv', 'str')
        """
        format_type = format_type.lower().strip()

        if format_type in ['excel', 'xlsx']:
            exporter = ExcelExporter()
        elif format_type in ['txt', 'text']:
            exporter = TxtExporter()
        elif format_type == 'csv':
            exporter = CsvExporter()
        elif format_type == 'str':
            exporter = StrExporter()
        else:
            raise ValueError(f"Unsupported format type: {format_type}")

        return exporter.export(gdf, title)
