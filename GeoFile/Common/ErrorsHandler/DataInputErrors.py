# GeoFile/Common/ErrorsHandler/DataInputErrors.py
"""
地理文件异常处理模块

为每一类地理文件输入中出现的异常提供合适的处理
"""

from pandas.errors import EmptyDataError, ParserError


class DataInputBaseErrorHandler:
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


class FileNotFoundHandler(DataInputBaseErrorHandler):
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


class ValueErrorHandler(DataInputBaseErrorHandler):
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


class CSVReadErrorHandler(DataInputBaseErrorHandler):
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


class ExcelReadErrorHandler(DataInputBaseErrorHandler):
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
        return DataInputBaseErrorHandler(file_path, error_obj)
