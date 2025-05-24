# GeoFile/Common/ErrorsHandler/data_input_errors.py
"""
地理文件异常处理模块

为每一类地理文件输入中出现的异常提供合适的处理
"""
import logging
import os
import shutil
import tempfile
import geopandas as gpd

from pyproj.exceptions import CRSError
from pyogrio.errors import DataSourceError
from pandas.errors import EmptyDataError, ParserError

from connection_manager import manager


class BaseErrorHandler:
    """异常处理基类"""
    ERROR_TYPE = Exception  # 基类默认处理所有异常

    def __init__(self, file_path, error_obj):
        self.file_path = file_path
        self.error_obj = error_obj
        self.error_info = {
            "原因": "未知错误",
            "技术诊断": [],
            "修复建议": []
        }

    def build_error_info(self):
        """构建错误信息（子类必须实现）"""
        self.error_info.update({
            "原因": "未知错误",
            "技术诊断": [
                f"原始错误: {str(self.error_obj)}",
            ],
            "修复建议": []
        })

    async def format_response(self):
        """统一格式化输出"""
        self.build_error_info()
        sections = [
            f"■ 错误原因\n{self.error_info['原因']}",
            "▼ 技术诊断\n" + "\n".join(self.error_info["技术诊断"]),
            "⚙ 修复建议\n" + "\n".join(self.error_info["修复建议"])
        ]
        return "\n".join(sections)


class FileNotFoundHandler(BaseErrorHandler):
    """文件不存在异常处理"""
    ERROR_TYPE = FileNotFoundError

    def build_error_info(self):
        self.error_info.update({
            "原因": "文件路径不存在",
            "技术诊断": [
                f"请求路径: {self.file_path}",
                f"系统报错: {str(self.error_obj)}",
                "可能原因:",
                "1. 文件路径包含特殊字符",
                "2. 文件已被移动或删除",
                "3. 使用了错误的相对路径"
            ],
            "修复建议": [
                "1. 检查路径中的中文字符或空格",
                "2. 尝试使用绝对路径",
                "3. 验证文件是否存在于指定位置"
            ]
        })


class ValueErrorHandler(BaseErrorHandler):
    """数值或参数异常处理"""
    ERROR_TYPE = ValueError

    def build_error_info(self):
        error_msg = str(self.error_obj).lower()
        reasons = []
        solutions = []

        if "1" in error_msg:
            reasons.append("未检测到经纬度字段")
            solutions.append("请明确指定列名/索引")
        elif "2" in error_msg:
            reasons.append("该文件类型是暂不支持的文件类型")
            solutions.append("尝试更换为shp/txt/excel数据重新上传")
        elif "3" in error_msg:
            reasons.append("无法自动识别坐标字段")
            solutions.append("请手动为文件添加或修改表头信息")
            solutions.append("将经度列命名为'经度', 'longitude', 'lon', 'x', 'X'中的一个")
            solutions.append("将纬度列命名为'纬度', 'latitude', 'lat', 'y', 'Y'中的一个")
        else:
            reasons.append(error_msg)
            solutions.append("请检查该值是否合规！")

        self.error_info.update({
            "原因": "文件重要值错误",
            "技术诊断": reasons,
            "修复建议": solutions
        })


class CRSErrorHandler(BaseErrorHandler):
    """坐标系异常处理"""
    ERROR_TYPE = CRSError

    def build_error_info(self):
        self.error_info.update({
            "原因": "坐标系定义异常",
            "技术诊断": [
                f"原始错误: {str(self.error_obj)}",
                "可能原因:",
                "1. PRJ文件缺失或损坏",
                "2. 使用了非标准EPSG代码",
                "3. 数据导出时未正确设置投影"
            ],
            "修复建议": [
                "正在尝试进行自动修复……"
            ]
        })

    async def format_response(self):
        self.build_error_info()
        sections = [
            f"■ 错误原因\n{self.error_info['原因']}",
            "▼ 技术诊断\n" + "\n".join(self.error_info["技术诊断"]),
            "⚙ 修复建议\n" + "\n".join(self.error_info["修复建议"])
        ]
        await manager.send_message("\n".join(sections))
        logging.info("\n".join(sections))

        """处理坐标系错误并尝试自动修复.prj文件问题"""
        prj_path = os.path.splitext(self.file_path)[0] + ".prj"
        temp_prj = None
        prj_removed = False

        # 尝试备份并移除.prj文件
        if os.path.exists(prj_path):
            temp_prj = tempfile.NamedTemporaryFile(delete=False, suffix=".prj")
            shutil.move(prj_path, temp_prj.name)
            prj_removed = True
            logging.info(f"已临时移除PRJ文件: {prj_path} → {temp_prj.name}")

        # 尝试重新读取数据（无.prj文件状态）
        try:
            gdf = gpd.read_file(self.file_path)
        except Exception as read_error:
            # 生成详细错误报告
            error_info = {
                "原因": "坐标系修复失败",
                "技术诊断": [
                    f"初次错误: {str(self.error_obj)}",
                    f"移除PRJ后错误: {str(read_error)}",
                    "可能原因:",
                    "1. 数据本身坐标值异常",
                    "2. 几何数据损坏",
                    "3. 需要手动指定坐标系"
                ],
                "修复建议": [
                    "终极方案：强制指定坐标系参数",
                    "操作步骤：",
                    "1. 用文本编辑器查看坐标值范围",
                    "2. 根据数据来源推测正确坐标系",
                    "3. 使用QGIS重新定义投影"
                ]
            }

            sections = [
                f"■ 错误原因\n{error_info['原因']}",
                "▼ 技术诊断\n" + "\n".join(error_info["技术诊断"]),
                "⚙ 修复建议\n" + "\n".join(error_info["修复建议"])
            ]
            return "\n".join(sections)

        await manager.send_message(
            f"成功读取文件 {os.path.basename(self.file_path)}\n"
            f"- 移除无效PRJ文件后坐标系: {gdf.crs}"
        )

        return gdf


class DataSourceErrorHandler(BaseErrorHandler):
    """数据源异常处理"""
    ERROR_TYPE = DataSourceError

    def build_error_info(self):
        error_msg = str(self.error_obj).lower()
        reasons = [error_msg]

        if "no such file" in error_msg:
            reasons.append("文件路径错误或包含特殊字符")
        elif "unrecognized data source" in error_msg:
            reasons.append("文件扩展名与实际格式不匹配")
        elif "failed to open" in error_msg:
            reasons.extend(["文件正在被其他程序占用", "文件权限不足"])
        elif ".shx" in error_msg:
            reasons.append("Shapefile组件不完整（缺少.shx文件）")
        else:
            reasons.append("未知数据源错误，需要进一步诊断")

        self.error_info.update({
            "原因": "数据源读取失败",
            "技术诊断": reasons,
            "修复建议": [
                "1. 检查文件完整性（必须包含.shp/.shx等）",
                "2. 验证文件编码：使用文本编辑器查看是否有乱码",
                "3. 尝试指定驱动参数：gpd.read_file(file_path, driver='ESRI Shapefile')",
                "4. 使用QGIS打开文件验证数据有效性"
            ]
        })


class CSVReadErrorHandler(BaseErrorHandler):
    """CSV读取异常处理"""
    ERROR_TYPE = (ParserError, EmptyDataError)

    def build_error_info(self):
        self.error_info.update({
            "原因": "CSV文件解析失败",
            "技术诊断": [
                f"错误类型: {type(self.error_obj).__name__}",
                f"详细消息: {str(self.error_obj)}"
            ],
            "修复建议": [
                "1. 检查文件编码格式（尝试GBK/UTF-8）",
                "2. 验证CSV文件是否损坏",
                "3. 检查分隔符是否统一"
            ]
        })


class ExcelReadErrorHandler(BaseErrorHandler):
    """Excel读取异常处理"""
    ERROR_TYPE = (ParserError, PermissionError)

    def build_error_info(self):
        self.error_info.update({
            "原因": "Excel文件读取失败",
            "技术诊断": [
                f"错误类型: {type(self.error_obj).__name__}",
                f"详细消息: {str(self.error_obj)}"
            ],
            "修复建议": [
                "1. 确认文件未被其他程序占用",
                "2. 验证Excel文件版本兼容性",
                "3. 尝试更改文件权限或更新Excel版本"
            ]
        })


class GeoFileErrorFactory:
    """异常处理工厂"""
    HANDLERS = {
        handler.ERROR_TYPE: handler
        for handler in [
            FileNotFoundHandler,
            CRSErrorHandler,
            DataSourceErrorHandler,
            CSVReadErrorHandler,
            ExcelReadErrorHandler,
            ValueErrorHandler
        ]
    }

    @classmethod
    def get_handler(cls, file_path, error_obj):
        """获取匹配的处理器"""
        for err_class, handler in cls.HANDLERS.items():
            if isinstance(error_obj, err_class):
                return handler(file_path, error_obj)
        return BaseErrorHandler(file_path, error_obj)
