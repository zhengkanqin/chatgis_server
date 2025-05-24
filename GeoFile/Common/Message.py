# GeoFile/Common/Message.py
"""
统一消息格式处理模块

提供标准化的成功/错误响应格式生成功能
"""
import logging

from connection_manager import manager


async def success(data: str):
    """
    生成标准化成功响应

    :param data: 需要返回的业务数据（支持任意可序列化类型）
    :return: 结构示例 {"status": "success", "data": ...}
    """
    await manager.send_message(data)
    return {"status": "success", "data": data}


async def error(message: str, error_code: str = None):
    """
    生成标准化错误响应

    :param message: 人类可读的错误描述
    :param error_code: 可选错误码（用于程序识别）
    :return: 结构示例 {"status": "error", "message": ..., "code": ...}
    """
    await manager.send_message(message)
    logging.error(f"出现错误: {str(message)}")
    response = {"status": "error", "message": message}
    if error_code:
        response["code"] = error_code
    return response
