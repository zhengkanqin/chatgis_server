# GeoFile/Common/ErrorsHandler.py
"""
异常处理模块

为出现的异常提供统一的处理
包含出错位置信息（工具名称+错误文件路径+行号）
"""
import os
import traceback
import sys
from pandas.errors import EmptyDataError, ParserError


class UnifiedBaseErrorHandler:
    """异常处理基类"""
    ERROR_TYPE = Exception  # 基类默认处理所有异常

    def __init__(self, tool_name, error_obj):
        """
        初始化异常处理器

        :param tool_name: 使用的工具/模块名称
        :param error_obj: 异常对象
        """
        self.tool_name = tool_name
        self.error_obj = error_obj

        # 获取错误位置信息
        self.error_location = self._get_error_location()

        self.error_info = {
            "原因": "未知错误",
            "技术诊断": [],
            "修复建议": []
        }

    @staticmethod
    def _get_error_location():
        """获取错误发生的文件路径和行号，专注于项目代码"""
        # 获取项目根目录路径 - 使用更可靠的方式
        if getattr(sys, 'frozen', False):
            # 如果是打包后的应用
            project_root = os.path.dirname(sys.executable)
        else:
            # 正常Python环境
            main_module = sys.modules['__main__'].__file__
            project_root = os.path.dirname(os.path.abspath(main_module))

        try:
            # 获取异常的回溯对象
            _, _, exc_tb = sys.exc_info()
            if exc_tb is None:
                return "无法获取回溯信息"

            # 收集所有项目内的栈帧
            project_frames = []
            current_tb = exc_tb

            while current_tb:
                frame = current_tb.tb_frame
                code = frame.f_code
                filename = code.co_filename

                # 只关注项目内的文件
                if filename.startswith(project_root):
                    # 排除虚拟环境和依赖库
                    if ('venv' not in filename and
                            '.venv' not in filename and
                            'site-packages' not in filename and
                            'dist-packages' not in filename):
                        # 获取相对路径
                        rel_path = os.path.relpath(filename, project_root)
                        project_frames.append((rel_path, current_tb.tb_lineno))

                current_tb = current_tb.tb_next

            # 优先返回最接近错误源的栈帧
            if project_frames:
                # 取最后一个栈帧（最接近错误源）
                rel_path, line_no = project_frames[-1]
                return f"文件: {rel_path} 行号: {line_no}"

            # 如果没有找到项目文件，返回最内层错误位置
            innermost_tb = exc_tb
            while innermost_tb.tb_next:
                innermost_tb = innermost_tb.tb_next

            filename = innermost_tb.tb_frame.f_code.co_filename
            line_no = innermost_tb.tb_lineno

            # 尝试简化路径显示
            try:
                rel_path = os.path.relpath(filename, project_root)
                return f"文件: {rel_path} 行号: {line_no}"
            except ValueError:
                return f"文件: {filename} 行号: {line_no}"

        except (AttributeError, TypeError, ValueError):
            # 更精确地捕获可能发生的异常
            try:
                # 使用traceback作为备选方案
                tb = traceback.extract_tb(sys.exc_info()[2])
                if tb:
                    # 尝试找到最后一个项目相关的帧
                    for frame in reversed(tb):
                        if project_root and frame.filename.startswith(project_root):
                            rel_path = os.path.relpath(frame.filename, project_root)
                            return f"文件: {rel_path} 行号: {frame.lineno}"

                    # 返回最后一个帧
                    last_frame = tb[-1]
                    return f"文件: {last_frame.filename} 行号: {last_frame.lineno}"
            finally:
                return "无法定位错误位置"

    def build_error_info(self):
        """构建错误信息（子类必须实现）"""
        self.error_info.update({
            "原因": "未知错误",
            "技术诊断": [
                f"原始错误: {str(self.error_obj)}",
            ],
            "修复建议": []
        })

    def format_response(self):
        """统一格式化输出"""
        self.build_error_info()
        sections = [
            f"▧ 出错位置\n工具: {self.tool_name} | 操作: {self.error_location}\n",
            f"■ 错误原因\n{self.error_info['原因']}",
            "▼ 技术诊断\n" + "\n".join(self.error_info["技术诊断"]),
            "⚙ 修复建议\n" + "\n".join(self.error_info["修复建议"])
        ]
        return "\n".join(sections)


class FileNotFoundHandler(UnifiedBaseErrorHandler):
    """文件不存在异常处理"""
    ERROR_TYPE = FileNotFoundError

    def build_error_info(self):
        self.error_info.update({
            "原因": "文件路径不存在",
            "技术诊断": [
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


class SpatialValueErrorHandler(UnifiedBaseErrorHandler):
    """空间操作数值异常处理"""
    ERROR_TYPE = ValueError

    def build_error_info(self):
        error_msg = str(self.error_obj).lower()
        reasons = []
        solutions = []

        if "图层仓库" in error_msg:
            reasons.append(error_msg)
            solutions.append("此文件不存在，请重新查阅图层仓库确定Layer名称是否正确！")
        elif "图层类型" in error_msg:
            reasons.append(error_msg)
            solutions.append("此文件不合法，图层类型无法被处理，请尝试使用其他类型的图层！")
        else:
            reasons.append(error_msg)
            solutions.append("请检查该值是否合规！")

        self.error_info.update({
            "原因": "空间操作重要值错误",
            "技术诊断": reasons,
            "修复建议": solutions
        })


class ShpValueErrorHandler(UnifiedBaseErrorHandler):
    """Shp操作数值异常处理"""
    ERROR_TYPE = ValueError

    def build_error_info(self):
        error_msg = str(self.error_obj).lower()
        reasons = []
        solutions = []

        if "操作类型" in error_msg:
            reasons.append(error_msg)
            solutions.append("请使用支持的操作方式！")
        elif "文件类型" in error_msg:
            reasons.append(error_msg)
            solutions.append("仅支持使用Shp文件格式！")
        elif "属性字段" in error_msg:
            reasons.append(error_msg)
            solutions.append("属性字段不存在，请重新阅读文件以确定属性字段名称是否正确！")
        elif "查询目标类型" in error_msg:
            reasons.append(error_msg)
            solutions.append("请检查对应参数是否是四种查询中的一种！")
        else:
            reasons.append(error_msg)
            solutions.append("请检查该值是否合规！")

        self.error_info.update({
            "原因": "Shp文件重要值错误",
            "技术诊断": reasons,
            "修复建议": solutions
        })


class DataInputValueErrorHandler(UnifiedBaseErrorHandler):
    """数据输入数值异常处理"""
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
            "原因": "数据输入重要值错误",
            "技术诊断": reasons,
            "修复建议": solutions
        })


class TypeErrorHandler(UnifiedBaseErrorHandler):
    """类型错误异常处理"""
    ERROR_TYPE = TypeError

    def build_error_info(self):
        error_msg = str(self.error_obj).lower()
        reasons = []
        solutions = []

        reasons.append(error_msg)
        solutions.append("请检查输入对象的类型是否合法！")

        self.error_info.update({
            "原因": "输入对象类型错误",
            "技术诊断": reasons,
            "修复建议": solutions
        })


class KeyErrorHandler(UnifiedBaseErrorHandler):
    """键值错误异常处理"""
    ERROR_TYPE = KeyError

    def build_error_info(self):
        error_msg = str(self.error_obj).lower()
        reasons = []
        solutions = []

        if 'coordinates' in error_msg:
            reasons.append(error_msg)
            solutions.append(
                "请检查输入的GeoJSON对象是否满足例如'{\"type\": \"Polygon\", \"coordinates\": [...]}'的可读取标准格式输入！")
        else:
            reasons.append(error_msg)
            solutions.append("请检查输入GeoJSON对象的键值是否合法！")

        self.error_info.update({
            "原因": "GeoJSON对象键值错误",
            "技术诊断": reasons,
            "修复建议": solutions
        })


class CSVReadErrorHandler(UnifiedBaseErrorHandler):
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


class ExcelReadErrorHandler(UnifiedBaseErrorHandler):
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


class UnifiedErrorFactory:
    """统一异常处理工厂"""
    HANDLERS = [
        FileNotFoundHandler,
        SpatialValueErrorHandler,
        ShpValueErrorHandler,
        DataInputValueErrorHandler,
        TypeErrorHandler,
        KeyErrorHandler,
        CSVReadErrorHandler,
        ExcelReadErrorHandler
    ]

    @classmethod
    def get_handler(cls, tool_name, error_obj):
        """获取匹配的处理器"""
        for error_handler in cls.HANDLERS:
            if isinstance(error_obj, error_handler.ERROR_TYPE):
                return error_handler(tool_name, error_obj)
        return UnifiedBaseErrorHandler(tool_name, error_obj)
