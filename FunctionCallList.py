# backend/FunctionCallList.py

from connection_manager import manager
from autogen_core.tools import FunctionTool
import json
from typing import Annotated
from pydantic import Field
#发送信息给前端
async def send_ws_message(message: str):
    """
        发送 WebSocket 消息。

        参数:
        - message (str): 要发送的消息内容。
    """
    await manager.send_message(message)
    print("发送成功")
    return "发送成功"

send_ws_message_tool = FunctionTool(
    send_ws_message,
    name="send_ws_message",
    description="Send a ws_message",
)


async def draw_city(name: str):
    """
    在地图上绘制城市的区域轮廓，帮助用户理解城市信息。

    参数:
    - name (str): 城市或者地区的名字
    """
    CommandEvent = {"type": "draw-city", "data": name}
    json_str = json.dumps(CommandEvent)  # 转成 JSON 字符串
    await manager.send_message(json_str)
    print(json_str)
    return "已经绘制成功"

draw_city_tool = FunctionTool(
    draw_city,
    name="draw_city",
    description="在用户可见的地图上绘制城市的区域轮廓，帮助用户理解城市信息。",
)