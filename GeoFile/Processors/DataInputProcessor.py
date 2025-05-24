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

    def __init__(self, file_path: str):
        super().__init__(file_path)
        self.lon_col = None
        self.lat_col = None
        self.header_mode = None
        self.df = None

    def detect_col(self, target_type: str):
        """
        智能检测坐标列
        :param target_type: 检测类型 ('lon'/'lat')
        :return: 列名或列索引
        """
        # 优先使用已指定的列
        col_attr = getattr(self, f"{target_type}_col")
        if col_attr is not None:
            return col_attr

        # 自动检测候选列
        auto_detect_map = {
            'lon': ['经度', 'longitude', 'lon', 'x', 'X'],
            'lat': ['纬度', 'latitude', 'lat', 'y', 'Y']
        }

        # 尝试匹配列名
        for candidate in auto_detect_map[target_type]:
            if candidate in self.df.columns:
                return candidate

        # 无表头模式返回默认列索引
        if not self.header_mode:
            return 0 if target_type == 'lon' else 1

        return None

    async def core(self):
        # ================= 文件验证 =================
        if not os.path.exists(self.file_path):
            raise FileNotFoundError
        file_ext = os.path.splitext(self.file_path)[1].lower()

        # ================= 数据加载 =================
        if file_ext in ['.xlsx', '.xls']:
            self.df = pd.read_excel(self.file_path)
        elif file_ext in ['.txt', '.csv']:
            sep = '\t' if file_ext == '.txt' else ','
            self.df = pd.read_csv(self.file_path, sep=sep, engine='python')

        # ================= 坐标字段检测 =================
        if self.lon_col is None or self.lat_col is None:
            self.header_mode = not self.df.columns.str.contains('^Unnamed').all()

            self.lon_col = self.detect_col('lon') or (0 if not self.header_mode else None)
            self.lat_col = self.detect_col('lat') or (1 if not self.header_mode else None)

        # ================= 智能识别增强 =================
        if self.lon_col is None or self.lat_col is None:
            # 获取所有数值列
            numeric_cols = self.df.select_dtypes(include=['number']).columns.tolist()

            # 中国地理坐标范围阈值
            CHINA_LON_RANGE = (73.66, 135.05)  # 东经73°40′~135°05′
            CHINA_LAT_RANGE = (18.15, 53.55)  # 北纬18°10′~53°33′

            # 遍历数值列寻找候选列
            candidate_pairs = []
            for i in range(len(numeric_cols) - 1):
                col1, col2 = numeric_cols[i], numeric_cols[i + 1]

                # 检查列对是否相邻（处理带表头和无表头情况）
                is_adjacent = (
                        (isinstance(col1, int) and isinstance(col2, int) and col2 == col1 + 1) or
                        (isinstance(col1, str) and isinstance(col2, str) and
                         col2.isdigit() and col1.isdigit() and int(col2) == int(col1) + 1)
                )

                if not is_adjacent:
                    continue

                # 检查数值范围
                col1_data = self.df[col1].dropna()
                col2_data = self.df[col2].dropna()

                col1_lon_candidate = (col1_data.between(*CHINA_LON_RANGE).mean() > 0.95)
                col2_lat_candidate = (col2_data.between(*CHINA_LAT_RANGE).mean() > 0.95)

                col1_lat_candidate = (col1_data.between(*CHINA_LAT_RANGE).mean() > 0.95)
                col2_lon_candidate = (col2_data.between(*CHINA_LON_RANGE).mean() > 0.95)

                # 记录符合条件的列对
                if col1_lon_candidate and col2_lat_candidate:
                    candidate_pairs.append((col1, col2))
                elif col1_lat_candidate and col2_lon_candidate:
                    candidate_pairs.append((col2, col1))

            # 选择最佳候选对
            if candidate_pairs:
                # 优先选择置信度最高的列对
                best_pair = max(candidate_pairs, key=lambda pair: (
                        self.df[pair[0]].between(*CHINA_LON_RANGE).mean() +
                        self.df[pair[1]].between(*CHINA_LAT_RANGE).mean()
                ))
                self.lon_col, self.lat_col = best_pair
                logging.info(f"智能选择坐标列：{self.lon_col}(经度), {self.lat_col}(纬度)")
            else:
                err_msg = "无法自动识别坐标字段，请手动指定lon_col和lat_col参数"
                await manager.send_message(err_msg)
                return await error(err_msg)
        # 验证坐标字段有效性
        for col, col_type in [(self.lon_col, "经度"), (self.lat_col, "纬度")]:
            if col is None:
                raise ValueError("1")

            try:
                self.df[col] = pd.to_numeric(self.df[col])
            except:
                err_msg = f"{col_type}字段 {col} 包含非数值数据"
                await manager.send_message(err_msg)
                return await error(err_msg)

        # ================= 数据分析 =================
        analysis = {
            "file_info": {
                "file_name": os.path.basename(self.file_path),
                "file_type": file_ext.strip('.'),
                "total_points": len(self.df),
                "coordinates_range": {
                    "min_lon": self.df[self.lon_col].min(),
                    "max_lon": self.df[self.lon_col].max(),
                    "min_lat": self.df[self.lat_col].min(),
                    "max_lat": self.df[self.lat_col].max()
                }
            },
            "attributes": {
                "fields": {},
                "system_fields": []
            }
        }

        # 处理每个字段
        for col in self.df.columns:
            if col in [self.lon_col, self.lat_col]:
                continue  # 跳过坐标字段

            dtype = self.df[col].dtype
            field_type = classify_field_type(dtype, self.df[col])
            stats = {}

            # 数值型处理
            if field_type in ["Double", "Float", "Short Integer", "Long Integer"]:
                stats = {
                    "type": field_type,
                    "min": self.df[col].min(),
                    "max": self.df[col].max(),
                    "mean": self.df[col].mean()
                }
            # 日期型处理
            elif field_type == "Date":
                stats = {
                    "type": "Date",
                    "min": self.df[col].min().strftime("%Y-%m-%d"),
                    "max": self.df[col].max().strftime("%Y-%m-%d")
                }
            # 文本型处理
            elif field_type == "Text":
                unique_values = self.df[col].dropna().unique()
                stats = {
                    "type": "Text",
                    "unique_count": len(unique_values),
                    "sample_values": unique_values.tolist()[:3] if len(unique_values) <= 3 else None
                }
            # 系统字段检测
            elif col.lower() in ['id', 'fid', 'oid']:
                analysis['attributes']['system_fields'].append({
                    "name": col,
                    "type": "Long Integer" if field_type in ["Short Integer", "Long Integer"] else field_type,
                    "count": len(self.df[col].unique())
                })
                continue

            analysis['attributes']['fields'][col] = stats

        # ================= 消息生成 =================
        output = [
            f"- 地理数据处理完成：{analysis['file_info']['file_name']}",
            "  - 数据概况：",
            f"    - 文件类型：{analysis['file_info']['file_type'].upper()}",
            f"    - 总点数：{analysis['file_info']['total_points']:,}",
            "  - 坐标范围：",
            f"    - 经度：{analysis['file_info']['coordinates_range']['min_lon']:.4f} ~ "
            f"{analysis['file_info']['coordinates_range']['max_lon']:.4f}",
            f"    - 纬度：{analysis['file_info']['coordinates_range']['min_lat']:.4f} ~ "
            f"{analysis['file_info']['coordinates_range']['max_lat']:.4f}",
            "  - 属性字段："
        ]

        # 添加字段信息
        for col, stats in analysis['attributes']['fields'].items():
            if stats['type'] in ["Double", "Float", "Short Integer", "Long Integer"]:
                output.append(
                    f"    - 数值字段 [{col}]（{stats['type']}）："
                    f"平均 {stats['mean']:.2f} | 最大 {stats['max']} | 最小 {stats['min']}"
                )
            elif stats['type'] == "Date":
                output.append(
                    f"    - 日期字段 [{col}]：范围 {stats['min']} 至 {stats['max']}"
                )
            elif stats['type'] == "Text":
                if stats['unique_count'] <= 3:
                    values = ', '.join(map(str, stats['sample_values']))
                    output.append(f"    - 文本字段 [{col}]：包含值 {values}")
                else:
                    output.append(f"    - 文本字段 [{col}]：唯一值数量 {stats['unique_count']}")

        # 添加系统字段
        for field in analysis['attributes']['system_fields']:
            output.append(
                f"    - 系统字段 [{field['name']}]：{field['type']}类型，共{field['count']}条记录"
            )

        # ================= 结果推送 =================
        result_msg = "\n".join(output)
        await manager.send_message(result_msg)

        logging.info(f"地理数据处理摘要：{json.dumps(analysis, indent=2)}")

        return await success(result_msg)


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
                raise ValueError("2")
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
