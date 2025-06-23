# GeoFile/Tools/ProximityTools.py

import os
from datetime import datetime
from typing import Dict, Optional, Union, Type

import geopandas as gpd
import numpy as np
import pandas as pd

from GeoFile.Tools.Gdf2TypeTool import ConverterFactory


class BaseProximityTool:
    """邻近分析基类"""

    def __init__(self,
                 main_gdf: gpd.GeoDataFrame,
                 near_gdf: Optional[gpd.GeoDataFrame] = None,
                 **kwargs):
        """
        初始化邻近分析工具

        参数:
        main_gdf: 主输入地理数据框
        near_gdf: 邻近要素地理数据框（可选）
        kwargs: 分析参数
        """
        self.main_gdf = main_gdf
        self.near_gdf = near_gdf if near_gdf is not None else main_gdf.copy()
        self.params = kwargs
        self.result = None

        # 确保坐标系一致
        if self.main_gdf.crs != self.near_gdf.crs:
            self.near_gdf = self.near_gdf.to_crs(self.main_gdf.crs)

    def execute(self) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
        """执行邻近分析"""
        raise NotImplementedError("子类必须实现此方法")

    def get_result_stats(self) -> Dict:
        """获取分析统计信息"""
        raise NotImplementedError("子类必须实现此方法")


class NearestNeighborAnalysis(BaseProximityTool):
    """近邻分析 - 计算每个要素到其最近邻要素的距离"""

    def execute(self) -> gpd.GeoDataFrame:
        # 计算所有几何的质心
        main_centroids = self.main_gdf.geometry.centroid
        near_centroids = self.near_gdf.geometry.centroid

        # 创建结果数据框
        result_gdf = self.main_gdf.copy()
        result_gdf['nearest_id'] = None
        result_gdf['distance'] = np.nan

        # 对每个要素查找最近邻
        for idx, main_row in result_gdf.iterrows():
            min_distance = float('inf')
            nearest_id = None

            # 计算到所有其他要素的距离
            for near_idx, near_row in self.near_gdf.iterrows():
                if idx == near_idx:  # 跳过自身
                    continue

                distance = main_centroids[idx].distance(near_centroids[near_idx])
                if distance < min_distance:
                    min_distance = distance
                    nearest_id = near_idx

            # 更新结果
            result_gdf.at[idx, 'nearest_id'] = nearest_id
            result_gdf.at[idx, 'distance'] = min_distance

        self.result = result_gdf
        return result_gdf

    def get_result_stats(self) -> Dict:
        if self.result is None:
            raise ValueError("请先执行近邻分析")

        stats = {
            "analysis_type": "Nearest Neighbor Analysis",
            "mean_distance": float(self.result['distance'].mean()),
            "min_distance": float(self.result['distance'].min()),
            "max_distance": float(self.result['distance'].max()),
            "feature_count": len(self.result)
        }
        return stats


class PolygonNeighbors(BaseProximityTool):
    """面邻域分析 - 识别相邻的面要素"""

    def execute(self) -> gpd.GeoDataFrame:
        # 创建缓冲区用于检测邻接关系
        buffer_distance = self.params.get('buffer_distance', 0.001)
        buffered = self.main_gdf.copy()
        buffered['buffer'] = buffered.geometry.buffer(buffer_distance)

        # 检测邻接关系
        neighbors_list = []
        for idx, row in buffered.iterrows():
            # 查找与当前面缓冲区相交的其他面
            possible_matches_index = list(buffered.sindex.intersection(row['buffer'].bounds))
            possible_matches = buffered.iloc[possible_matches_index]
            precise_matches = possible_matches[possible_matches.geometry.intersects(row['buffer'])]

            # 排除自身
            neighbors = precise_matches[precise_matches.index != idx]

            # 记录邻接关系
            for neighbor_idx in neighbors.index:
                neighbors_list.append({
                    'source_id': idx,
                    'neighbor_id': neighbor_idx,
                    'shared_boundary_length': row.geometry.boundary.intersection(
                        neighbors.loc[neighbor_idx].geometry.boundary).length
                })

        # 创建结果DataFrame
        neighbors_df = pd.DataFrame(neighbors_list)

        # 合并回主数据框
        result_gdf = self.main_gdf.copy()
        neighbors_grouped = neighbors_df.groupby('source_id')['neighbor_id'].apply(list).reset_index()
        neighbors_grouped.columns = ['source_id', 'neighbors']

        result_gdf = result_gdf.merge(
            neighbors_grouped,
            left_index=True,
            right_on='source_id',
            how='left'
        )

        # 添加邻接面数量
        result_gdf['neighbor_count'] = result_gdf['neighbors'].apply(
            lambda x: len(x) if isinstance(x, list) else 0)

        self.result = result_gdf
        return result_gdf

    def get_result_stats(self) -> Dict:
        if self.result is None:
            raise ValueError("请先执行面邻域分析")

        stats = {
            "analysis_type": "Polygon Neighbors",
            "total_adjacencies": int(self.result['neighbor_count'].sum()),
            "mean_neighbors": float(self.result['neighbor_count'].mean()),
            "isolated_features": int((self.result['neighbor_count'] == 0).sum()),
            "feature_count": len(self.result)
        }
        return stats


class GenerateNearTable(BaseProximityTool):
    """生成近邻表 - 计算所有要素对之间的距离"""

    def execute(self) -> pd.DataFrame:
        # 计算所有几何的质心
        main_centroids = self.main_gdf.geometry.centroid
        near_centroids = self.near_gdf.geometry.centroid

        # 获取最大距离参数
        max_distance = self.params.get('max_distance', None)

        # 查找所有邻近对
        near_table = []
        for i, (main_idx, main_row) in enumerate(self.main_gdf.iterrows()):
            for j, (near_idx, near_row) in enumerate(self.near_gdf.iterrows()):
                # 排除自身（如果是同一个数据集）
                if self.main_gdf is self.near_gdf and i == j:
                    continue

                # 计算距离
                distance = main_centroids[i].distance(near_centroids[j])

                # 检查距离限制
                if max_distance is not None and distance > max_distance:
                    continue

                near_table.append({
                    'IN_FID': main_idx,
                    'NEAR_FID': near_idx,
                    'DISTANCE': distance
                })

        # 创建结果DataFrame
        result_df = pd.DataFrame(near_table)

        # 添加属性信息
        if self.params.get('include_attributes', True):
            for col in self.main_gdf.columns:
                if col not in ['geometry']:
                    result_df[f'IN_{col}'] = result_df['IN_FID'].map(self.main_gdf[col])

            for col in self.near_gdf.columns:
                if col not in ['geometry']:
                    result_df[f'NEAR_{col}'] = result_df['NEAR_FID'].map(self.near_gdf[col])

        self.result = result_df
        return result_df

    def get_result_stats(self) -> Dict:
        if self.result is None:
            raise ValueError("请先执行近邻表生成")

        stats = {
            "analysis_type": "Generate Near Table",
            "total_pairs": len(self.result),
            "mean_distance": float(self.result['DISTANCE'].mean()),
            "min_distance": float(self.result['DISTANCE'].min()),
            "max_distance": float(self.result['DISTANCE'].max()),
            "max_distance_set": self.params.get('max_distance', 'None')
        }
        return stats


class PointDistance(BaseProximityTool):
    """点距离分析 - 计算两点数据集之间的距离"""

    def execute(self) -> pd.DataFrame:
        # 确保输入是点要素
        if not all(geom.geom_type == 'Point' for geom in self.main_gdf.geometry):
            raise ValueError("主输入必须是点要素")
        if not all(geom.geom_type == 'Point' for geom in self.near_gdf.geometry):
            raise ValueError("邻近输入必须是点要素")

        # 获取最大距离参数
        max_distance = self.params.get('max_distance', None)

        # 查找所有邻近对
        result_list = []
        for main_idx, main_row in self.main_gdf.iterrows():
            min_distance = float('inf')
            nearest_point = None

            # 查找最近的邻近点
            for near_idx, near_row in self.near_gdf.iterrows():
                distance = main_row.geometry.distance(near_row.geometry)

                # 检查距离限制
                if max_distance is not None and distance > max_distance:
                    continue

                if distance < min_distance:
                    min_distance = distance
                    nearest_point = near_row

            # 如果找到邻近点
            if nearest_point is not None:
                result_list.append({
                    'FROM_ID': main_idx,
                    'TO_ID': nearest_point.name,
                    'DISTANCE': min_distance,
                    'FROM_X': main_row.geometry.x,
                    'FROM_Y': main_row.geometry.y,
                    'TO_X': nearest_point.geometry.x,
                    'TO_Y': nearest_point.geometry.y
                })

        # 创建结果DataFrame
        result_df = pd.DataFrame(result_list)

        # 添加属性信息
        if self.params.get('include_attributes', True):
            for col in self.main_gdf.columns:
                if col != 'geometry':
                    result_df[f'FROM_{col}'] = result_df['FROM_ID'].map(self.main_gdf[col])

            for col in self.near_gdf.columns:
                if col != 'geometry':
                    result_df[f'TO_{col}'] = result_df['TO_ID'].map(self.near_gdf[col])

        self.result = result_df
        return result_df

    def get_result_stats(self) -> Dict:
        if self.result is None:
            raise ValueError("请先执行点距离分析")

        stats = {
            "analysis_type": "Point Distance",
            "total_pairs": len(self.result),
            "mean_distance": float(self.result['DISTANCE'].mean()),
            "min_distance": float(self.result['DISTANCE'].min()),
            "max_distance": float(self.result['DISTANCE'].max()),
            "max_distance_set": self.params.get('max_distance', 'None')
        }
        return stats


class ProximityFactory:
    """邻近分析工厂类"""

    PROXIMITY_REGISTRY = {
        'nearest_neighbor': NearestNeighborAnalysis,
        'polygon_neighbors': PolygonNeighbors,
        'generate_near_table': GenerateNearTable,
        'point_distance': PointDistance
    }

    @classmethod
    def get_proximity_tool(cls,
                           tool_name: str,
                           main_gdf: gpd.GeoDataFrame,
                           near_gdf: Optional[gpd.GeoDataFrame] = None,
                           **kwargs) -> BaseProximityTool:
        """
        获取邻近分析工具

        参数:
        tool_name: 工具名称
        main_gdf: 主输入地理数据框
        near_gdf: 邻近要素地理数据框（可选）
        kwargs: 工具特定参数

        返回:
        对应的邻近分析工具实例
        """
        tool_class = cls.PROXIMITY_REGISTRY.get(tool_name.lower())
        if not tool_class:
            raise ValueError(f"暂不支持的邻近分析工具: {tool_name}")

        return tool_class(main_gdf, near_gdf, **kwargs)

    @classmethod
    def register_tool(cls, tool_name: str, tool_class: Type[BaseProximityTool]):
        """注册新的邻近分析工具"""
        if not issubclass(tool_class, BaseProximityTool):
            raise TypeError("邻近分析工具必须是BaseProximityTool的子类")
        cls.PROXIMITY_REGISTRY[tool_name.lower()] = tool_class


class ProximityResultProcessor:
    """邻近分析结果处理器"""

    def __init__(self,
                 result: Union[gpd.GeoDataFrame, pd.DataFrame],
                 stats: Dict,
                 tool_name: str):
        """
        初始化结果处理器

        参数:
        result: 分析结果（空间数据框或表格）
        stats: 分析统计信息
        tool_name: 使用的工具名称
        """
        self.result = result
        self.stats = stats
        self.tool_name = tool_name

    def generate_outputs(self) -> Dict:
        """生成所有输出结果"""
        # 创建输出目录
        output_dir = os.path.abspath("GeoFile/Result/_proximity_result")
        os.makedirs(output_dir, exist_ok=True)

        # 生成GeoJSON（如果是空间数据）
        geojson_path = None
        shp_path = None

        if isinstance(self.result, gpd.GeoDataFrame):
            geojson_path = self._generate_geojson(output_dir)
            shp_path = self._generate_shapefile(output_dir)

        # 生成CSV（如果是表格数据）
        csv_path = None
        if isinstance(self.result, pd.DataFrame):
            csv_path = self._generate_csv(output_dir)

        # 生成统计信息
        stats_str = self._generate_stats_string(geojson_path, shp_path, csv_path)

        return {
            "geojson_path": geojson_path,
            "shp_path": shp_path,
            "csv_path": csv_path,
            "stats": stats_str,
            "analysis_stats": self.stats
        }

    def _generate_geojson(self, output_dir: str) -> str:
        """生成GeoJSON文件"""
        converter = ConverterFactory.get_converter(
            type_name='geojson',
            gdf=self.result,
            custom_output_path=output_dir
        )
        geojson_path, _, _ = converter.convert()
        return geojson_path

    def _generate_shapefile(self, output_dir: str) -> str:
        """生成Shapefile文件"""
        converter = ConverterFactory.get_converter(
            type_name='shp',
            gdf=self.result,
            custom_output_path=output_dir
        )
        shp_path, _, _, _ = converter.convert()
        return shp_path

    def _generate_csv(self, output_dir: str) -> str:
        """生成CSV文件"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"{self.tool_name}_result_{timestamp}.csv"
        csv_path = os.path.join(output_dir, csv_filename)

        self.result.to_csv(csv_path, index=False)
        return csv_path

    def _generate_stats_string(self,
                               geojson_path: Optional[str],
                               shp_path: Optional[str],
                               csv_path: Optional[str]) -> str:
        """生成结果统计字符串"""
        tool_name = self.tool_name.replace('_', ' ').title()
        result = f"{tool_name} 分析完成\n"
        result += f"分析类型: {self.stats['analysis_type']}\n"

        # 添加文件路径信息
        if geojson_path:
            result += f"\n空间结果文件:\n"
            result += f"  - GeoJSON: {geojson_path}\n"
            result += f"  - Shapefile: {shp_path}\n"

        if csv_path:
            result += f"\n表格结果文件:\n"
            result += f"  - CSV: {csv_path}\n"

        # 添加统计信息
        result += "\n分析统计:\n"
        for key, value in self.stats.items():
            if key == 'analysis_type':
                continue

            # 格式化键名
            formatted_key = key.replace('_', ' ').title()

            # 特殊处理某些统计值
            if 'distance' in key and isinstance(value, float):
                result += f"  {formatted_key}: {value:.2f} 单位\n"
            else:
                result += f"  {formatted_key}: {value}\n"

        # 添加特定工具的解释
        if self.tool_name == 'nearest_neighbor':
            result += "\n说明: 该分析计算了每个要素到其最近邻要素的距离"
        elif self.tool_name == 'polygon_neighbors':
            result += "\n说明: 该分析识别了面要素之间的邻接关系"
        elif self.tool_name == 'generate_near_table':
            result += "\n说明: 该分析计算了所有要素对之间的距离"
        elif self.tool_name == 'point_distance':
            result += "\n说明: 该分析计算了点要素之间的距离"

        return result
