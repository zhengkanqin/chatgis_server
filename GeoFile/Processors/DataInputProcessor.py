# GeoFile/Processors/DataInputProcessor.py
"""
统一消息格式处理模块

提供标准化的成功/错误响应格式生成功能
"""
import json
import logging
import os
import pandas as pd
import geopandas as gpd
from abc import ABC, abstractmethod

from GeoFile.Common.ErrorsHandler.DataInputErrors import GeoFileErrorFactory
from GeoFile.Common.Message import success
from GeoFile.Common.Message import error
from GeoFile.Tools.DataInputTools import classify_field_type
from connection_manager import manager


class BaseFileProcessor(ABC):
    """文件处理器基类"""

    SUPPORTED_EXTENSIONS = []

    def __init__(self, file_path: str):
        """
        :param file_path: 文件路径（支持绝对/相对路径）
        """
        self.file_path = os.path.abspath(file_path)
        self._validate()

    def _validate(self):
        """基础验证"""
        if not self._check_extension():
            raise ValueError(f"不支持的文件类型: {self.extension}")

    @property
    def extension(self) -> str:
        """获取文件扩展名"""
        return os.path.splitext(self.file_path)[1].lower()

    def _check_extension(self) -> bool:
        """检查扩展名是否支持"""
        return self.extension in self.SUPPORTED_EXTENSIONS

    @abstractmethod
    async def core(self):
        """处理入口方法（需子类实现）"""
        pass


class ShpProcessor(BaseFileProcessor):
    """Shapefile处理器"""

    SUPPORTED_EXTENSIONS = ['.shp']

    async def core(self):
        gdf = gpd.read_file(self.file_path)
        return await self.process(gdf)

    async def process(self, gdf):
        # 计算坐标范围
        bounds = gdf.total_bounds
        coord_range = {
            "min_lon": bounds[0],
            "max_lon": bounds[2],
            "min_lat": bounds[1],
            "max_lat": bounds[3]
        }

        # 构建特征摘要
        summary = {
            "file_info": {
                "file_name": os.path.basename(self.file_path),
                "crs": str(gdf.crs),
                "geometry_type": gdf.geometry.type.unique().tolist(),
                "total_features": len(gdf),
                "coord_range": coord_range
            },
            "attributes": {
                "fields": {},
                "special_fields": {
                    "Geometry": [],
                    "GUID": [],
                    "ObjectID": []
                }
            }
        }

        # 分析每个字段
        for col in gdf.columns:
            if col == 'geometry':
                summary['attributes']['special_fields']['Geometry'].append({
                    "type": "Geometry",
                    "count": len(gdf)
                })
                continue

            field_type = classify_field_type(gdf[col].dtype, gdf[col])
            stats = {}

            # 数值型处理
            if field_type in ["Float", "Double", "Short Integer", "Long Integer"]:
                stats = {
                    "type": field_type,
                    "min": gdf[col].min(),
                    "max": gdf[col].max(),
                    "mean": gdf[col].mean()
                }
            # 文本型处理
            elif field_type == "Text":
                unique_values = gdf[col].dropna().unique()
                stats = {
                    "type": "Text",
                    "unique_count": len(unique_values),
                    "sample_values": unique_values.tolist()[:5] if len(unique_values) <= 5 else None
                }
            # 日期型处理
            elif field_type == "Date":
                stats = {
                    "type": "Date",
                    "min": gdf[col].min().strftime("%Y-%m-%d"),
                    "max": gdf[col].max().strftime("%Y-%m-%d")
                }
            # 特殊字段处理
            elif col.lower() in ['fid', 'objectid']:
                summary['attributes']['special_fields']['ObjectID'].append({
                    "type": "Long Integer",
                    "count": len(gdf[col].unique())
                })
                continue

            summary['attributes']['fields'][col] = stats

        # 格式化输出消息
        output = [
            f"- 地理数据处理完成：{summary['file_info']['file_name']}",
            "  - 数据概况：",
            f"    - 文件类型：SHP",
            f"    - 坐标系：{summary['file_info']['crs']}",
            f"    - 几何类型：{', '.join(summary['file_info']['geometry_type'])}",
            f"    - 要素总数：{summary['file_info']['total_features']}",
            "  - 坐标范围：",
            f"    - 经度：{summary['file_info']['coord_range']['min_lon']:.4f} ~ {summary['file_info']['coord_range']['max_lon']:.4f}",
            f"    - 纬度：{summary['file_info']['coord_range']['min_lat']:.4f} ~ {summary['file_info']['coord_range']['max_lat']:.4f}",
            "  - 属性字段："
        ]

        # 添加字段详细信息
        for col, stats in summary['attributes']['fields'].items():
            if stats['type'] in ["Float", "Double", "Short Integer", "Long Integer"]:
                output.append(
                    f"    - 数值字段 [{col}]（{stats['type']}）："
                    f"平均 {stats['mean']:.2f} | 最大 {stats['max']} | 最小 {stats['min']}"
                )
            elif stats['type'] == "Text":
                if stats['unique_count'] <= 5:
                    values = ', '.join(map(str, stats['sample_values']))
                    output.append(f"    - 文本字段 [{col}]：包含值 {values}")
                else:
                    output.append(f"    - 文本字段 [{col}]：唯一值数量 {stats['unique_count']}")
            elif stats['type'] == "Date":
                output.append(
                    f"    - 日期字段 [{col}]：范围 {stats['min']} 至 {stats['max']}"
                )

        # 添加特殊字段
        for field_type, entries in summary['attributes']['special_fields'].items():
            for entry in entries:
                output.append(
                    f"    - 系统字段 [{field_type}]：{entry['type']}类型，共{entry['count']}条记录"
                )

        # 发送处理结果
        result_msg = "\n".join(output)
        await manager.send_message(result_msg)

        # 记录原始数据
        logging.info(f"SHP处理原始数据：{json.dumps(summary, indent=2)}")

        return {"status": "success", "data": result_msg}


class TabularProcessor(BaseFileProcessor):
    """表格数据处理器（CSV/TXT/Excel）"""

    SUPPORTED_EXTENSIONS = ['.csv', '.txt', '.xlsx', '.xls']

    async def core(self):
        try:
            if self.extension in ['.csv', '.txt']:
                df = pd.read_csv(self.file_path)
            else:
                df = pd.read_excel(self.file_path)

            return {
                "type": "tabular",
                "rows": len(df),
                "columns": list(df.columns),
                "sample_data": df.head().to_dict()
            }
        except Exception as e:
            return {"error": f"表格处理失败: {str(e)}"}


class FileProcessorFactory:
    """文件处理器工厂"""

    PROCESSORS = {
        **{ext: ShpProcessor for ext in ShpProcessor.SUPPORTED_EXTENSIONS},
        **{ext: TabularProcessor for ext in TabularProcessor.SUPPORTED_EXTENSIONS}
    }

    @classmethod
    async def create_processor(cls, file_path: str):
        """创建处理器实例"""
        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext not in cls.PROCESSORS:
                raise ValueError
            processor_class = cls.PROCESSORS[ext]
            processor = processor_class(file_path)
            try:
                process_result = await processor.core()
            except Exception as e:
                handler = GeoFileErrorFactory.get_handler(file_path, e)
                response = await handler.format_response()
                if isinstance(response, str):
                    return await error(response)
                else:
                    process_result = await processor.process()
            return await success(process_result)
        except Exception as e:
            handler = GeoFileErrorFactory.get_handler(file_path, e)
            response = await handler.format_response()
            return await error(response)
