# GeoFile/Tools/ClusterAnalysisTool.py
import os
from datetime import datetime
from typing import List, Optional, Dict, Type

import geopandas as gpd
import numpy as np
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

from GeoFile.Tools.Gdf2TypeTool import ConverterFactory


class BaseCluster:
    """聚类分析基类，定义公共接口"""

    def __init__(self, gdf: gpd.GeoDataFrame, use_attributes: Optional[List[str]] = None, **kwargs):
        """
        初始化聚类器

        参数:
        gdf: 输入的地理数据框
        use_attributes: 用于聚类的属性字段列表
        kwargs: 算法特定参数
        """
        self.gdf = gdf.copy()
        self.use_attributes = use_attributes
        self.params = kwargs
        self.model = None
        self.labels = None
        self.features = self._prepare_features()

    def _prepare_features(self) -> np.ndarray:
        """准备特征矩阵，包括空间坐标和属性"""
        features = []

        # 添加几何坐标（点/面数据自适应处理）
        if self.gdf.geom_type.iloc[0] == 'Point':
            coords = [[p.x, p.y] for p in self.gdf.geometry]
        else:
            coords = [[p.centroid.x, p.centroid.y] for p in self.gdf.geometry]

        features.append(coords)

        # 添加属性字段（如果指定）
        if self.use_attributes:
            for attr in self.use_attributes:
                if attr in self.gdf.columns:
                    features.append(self.gdf[attr].values.reshape(-1, 1))

        # 合并特征并标准化
        X = np.hstack(features)
        return StandardScaler().fit_transform(X)

    def fit_predict(self) -> gpd.GeoDataFrame:
        """执行聚类并返回带标签的GeoDataFrame"""
        raise NotImplementedError("子类必须实现此方法")

    def get_cluster_stats(self) -> Dict:
        """获取聚类统计信息"""
        if self.labels is None:
            raise ValueError("请先执行聚类分析")

        unique_labels = np.unique(self.labels)
        cluster_sizes = {int(label): int(np.sum(self.labels == label)) for label in unique_labels}

        stats = {
            "algorithm": type(self).__name__.replace("Cluster", "").lower(),
            "n_clusters": len(unique_labels),
            "cluster_sizes": cluster_sizes,
            "noise_points": int(np.sum(self.labels == -1)) if -1 in self.labels else 0
        }

        # 计算轮廓系数（如果簇数大于1且没有噪声点）
        if len(unique_labels) > 1 and not np.any(self.labels == -1):
            try:
                stats["silhouette_score"] = float(silhouette_score(self.features, self.labels))
            finally:
                pass

        return stats


class KMeansCluster(BaseCluster):
    """K均值聚类实现"""

    def fit_predict(self) -> gpd.GeoDataFrame:
        from sklearn.cluster import KMeans

        n_clusters = self.params.get('n_clusters', 5)
        self.model = KMeans(
            n_clusters=n_clusters,
            init='k-means++',
            n_init=10,
            random_state=42
        )

        self.labels = self.model.fit_predict(self.features)
        self.gdf['cluster'] = self.labels
        return self.gdf


class DBSCANCluster(BaseCluster):
    """DBSCAN密度聚类实现"""

    def fit_predict(self) -> gpd.GeoDataFrame:
        from sklearn.cluster import DBSCAN

        eps = self.params.get('eps', 0.2)
        min_samples = self.params.get('min_samples', 5)

        self.model = DBSCAN(eps=eps, min_samples=min_samples)
        self.labels = self.model.fit_predict(self.features)
        self.gdf['cluster'] = self.labels
        return self.gdf


class AgglomerativeCluster(BaseCluster):
    """层次聚类实现"""

    def fit_predict(self) -> gpd.GeoDataFrame:
        from sklearn.cluster import AgglomerativeClustering

        n_clusters = self.params.get('n_clusters', 5)
        linkage = self.params.get('linkage', 'ward')

        self.model = AgglomerativeClustering(
            n_clusters=n_clusters,
            linkage=linkage
        )

        self.labels = self.model.fit_predict(self.features)
        self.gdf['cluster'] = self.labels
        return self.gdf


class MeanShiftCluster(BaseCluster):
    """均值漂移聚类实现"""

    def fit_predict(self) -> gpd.GeoDataFrame:
        from sklearn.cluster import MeanShift

        bandwidth = self.params.get('bandwidth', None)
        self.model = MeanShift(bandwidth=bandwidth, cluster_all=False)
        self.labels = self.model.fit_predict(self.features)
        self.gdf['cluster'] = self.labels
        return self.gdf


class SpectralCluster(BaseCluster):
    """谱聚类实现"""

    def fit_predict(self) -> gpd.GeoDataFrame:
        from sklearn.cluster import SpectralClustering

        n_clusters = self.params.get('n_clusters', 5)
        affinity = self.params.get('affinity', 'rbf')

        self.model = SpectralClustering(
            n_clusters=n_clusters,
            affinity=affinity,
            random_state=42
        )

        self.labels = self.model.fit_predict(self.features)
        self.gdf['cluster'] = self.labels
        return self.gdf


class OPTICSCluster(BaseCluster):
    """OPTICS密度聚类实现"""

    def fit_predict(self) -> gpd.GeoDataFrame:
        from sklearn.cluster import OPTICS

        min_samples = self.params.get('min_samples', 10)
        xi = self.params.get('xi', 0.05)

        self.model = OPTICS(min_samples=min_samples, xi=xi)
        self.labels = self.model.fit_predict(self.features)
        self.gdf['cluster'] = self.labels
        return self.gdf


class ClusterFactory:
    """聚类工厂类，根据算法名称创建对应的聚类器"""

    # 注册的聚类器映射
    CLUSTER_REGISTRY = {
        'kmeans': KMeansCluster,
        'dbscan': DBSCANCluster,
        'agglomerative': AgglomerativeCluster,
        'meanshift': MeanShiftCluster,
        'spectral': SpectralCluster,
        'optics': OPTICSCluster
    }

    @classmethod
    def get_cluster(cls,
                    algorithm: str,
                    gdf: gpd.GeoDataFrame,
                    use_attributes: Optional[List[str]] = None,
                    **kwargs) -> BaseCluster:
        """
        获取指定算法的聚类器

        参数:
        algorithm: 聚类算法名称 (如 'kmeans', 'dbscan')
        gdf: 包含地理数据的GeoDataFrame
        use_attributes: 用于聚类的属性字段列表
        kwargs: 算法特定参数

        返回:
        对应的聚类器实例
        """
        cluster_class = cls.CLUSTER_REGISTRY.get(algorithm.lower())
        if not cluster_class:
            raise ValueError(f"暂不支持的聚类算法: {algorithm}")

        return cluster_class(gdf, use_attributes=use_attributes, **kwargs)

    @classmethod
    def register_cluster(cls, algorithm: str, cluster_class: Type[BaseCluster]):
        """
        注册新的聚类器类型

        参数:
        algorithm: 聚类算法名称
        cluster_class: 聚类器类（必须是BaseCluster的子类）
        """
        if not issubclass(cluster_class, BaseCluster):
            raise TypeError("聚类器必须是BaseCluster的子类")

        cls.CLUSTER_REGISTRY[algorithm.lower()] = cluster_class


class ClusterResultProcessor:
    """聚类结果处理器，负责生成输出文件和统计信息"""

    def __init__(self, clustered_gdf: gpd.GeoDataFrame, cluster_stats: Dict):
        """
        初始化结果处理器

        参数:
        clustered_gdf: 包含聚类标签的GeoDataFrame
        cluster_stats: 聚类统计信息
        """
        self.clustered_gdf = clustered_gdf
        self.cluster_stats = cluster_stats

    def generate_outputs(self) -> Dict:
        """生成所有输出结果"""
        # 创建目录存储输出文件
        output_dir = os.path.abspath(f"GeoFile/Result/_cluster_result")

        # 准备输出目录
        os.makedirs(output_dir, exist_ok=True)

        # 生成GeoJSON
        geojson_path = self._generate_geojson(output_dir)

        # 生成Shapefile
        shp_path = self._generate_shapefile(output_dir)

        # 生成统计信息字符串，包含文件路径
        stats_str = self._generate_stats_string(geojson_path, shp_path)

        return {
            "geojson_path": geojson_path,
            "shp_path": shp_path,
            "stats": stats_str,
            "cluster_stats": self.cluster_stats
        }

    def _generate_geojson(self, output_path: str):
        """生成GeoJSON文件"""
        # 使用转换器工厂生成GeoJSON
        converter = ConverterFactory.get_converter(
            type_name='geojson',
            gdf=self.clustered_gdf,
            custom_output_path=output_path
        )
        geojson_path, _, _ = converter.convert()

        return geojson_path

    def _generate_shapefile(self, output_path: str):
        """生成Shapefile文件"""
        # 使用转换器工厂生成Shapefile
        converter = ConverterFactory.get_converter(
            type_name='shp',
            gdf=self.clustered_gdf,
            custom_output_path=output_path
        )
        shp_path, _, _, _ = converter.convert()

        return shp_path

    def _generate_stats_string(self, geojson_path: str, shp_path: str) -> str:
        """生成包含文件路径和聚类统计信息的字符串"""
        stats = self.cluster_stats
        algorithm_name = stats["algorithm"].upper()

        # 文件路径信息
        result = f"聚类结果文件已生成：\n"
        result += f"  - GeoJSON: {geojson_path}\n"
        result += f"  - Shapefile: {shp_path}\n"

        # 聚类算法信息
        result += f"聚类算法: {algorithm_name}\n"
        result += f"聚类数量: {stats['n_clusters']}\n"

        # 噪声点信息（如果存在）
        if stats.get('noise_points', 0) > 0:
            result += f"噪声点数量: {stats['noise_points']}\n"

        # 簇大小分布
        result += "\n簇大小分布:\n"
        for cluster_id, size in stats["cluster_sizes"].items():
            result += f"  簇 {cluster_id}: {size} 个要素\n"

        # 轮廓系数
        if 'silhouette_score' in stats:
            score = stats['silhouette_score']
            result += f"\n轮廓系数: {score:.3f}\n"
            if score > 0.7:
                result += "  → 聚类结构优秀\n"
            elif score > 0.5:
                result += "  → 聚类结构良好\n"
            elif score > 0.25:
                result += "  → 聚类结构一般\n"
            else:
                result += "  → 聚类结构较差\n"

        # 算法特定信息
        if algorithm_name == "DBSCAN":
            result += "\nDBSCAN参数:\n"
            result += f"  eps: {self.cluster_stats.get('params', {}).get('eps', 0.2)}\n"
            result += f"  min_samples: {self.cluster_stats.get('params', {}).get('min_samples', 5)}\n"

        elif algorithm_name == "KMEANS":
            result += "\nK均值参数:\n"
            result += f"  初始中心点算法: k-means++\n"
            result += f"  迭代次数: 300\n"

        return result
