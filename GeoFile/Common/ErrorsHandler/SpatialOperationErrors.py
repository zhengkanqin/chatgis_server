# GeoFile/Common/ErrorsHandler/SpatialOperationErrors.py
"""
空间操作异常处理模块

为空间操作中出现的异常提供合适的处理
"""


class SpatialBaseErrorHandler:
    """异常处理基类"""
    ERROR_TYPE = Exception  # 基类默认处理所有异常

    def __init__(self, operation, params, error_obj):
        self.operation = operation
        self.params = params
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


class FileNotFoundHandler(SpatialBaseErrorHandler):
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


class ValueErrorHandler(SpatialBaseErrorHandler):
    """数值或参数异常处理"""
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
            "原因": "文件重要值错误",
            "技术诊断": reasons,
            "修复建议": solutions
        })


class TypeErrorHandler(SpatialBaseErrorHandler):
    """数值或参数异常处理"""
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


class KeyErrorHandler(SpatialBaseErrorHandler):
    """数值或参数异常处理"""
    ERROR_TYPE = KeyError

    def build_error_info(self):
        error_msg = str(self.error_obj).lower()
        reasons = []
        solutions = []

        if 'coordinates' in error_msg:
            reasons.append(error_msg)
            solutions.append("请检查输入的GeoJSON对象是否满足例如'{\"type\": \"Polygon\", \"coordinates\": [...]}'等的可读取标准格式输入！")
        else:
            reasons.append(error_msg)
            solutions.append("请检查输入GeoJSON对象的键值是否合法！")

        self.error_info.update({
            "原因": "GeoJSON对象键值错误",
            "技术诊断": reasons,
            "修复建议": solutions
        })


class SpatialOperationErrorFactory:
    """异常处理工厂"""
    HANDLERS = {
        handler.ERROR_TYPE: handler
        for handler in [
            FileNotFoundHandler,
            ValueErrorHandler,
            TypeErrorHandler,
            KeyErrorHandler
        ]
    }

    @classmethod
    def get_handler(cls, operation, params, error_obj):
        """获取匹配的处理器"""
        for err_class, handler in cls.HANDLERS.items():
            if isinstance(error_obj, err_class):
                return handler(operation, params, error_obj)
        return SpatialBaseErrorHandler(operation, params, error_obj)
