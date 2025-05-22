# backend/FunctionCall_List.py

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


async def draw_boundary(name: str):
    """
    在地图上绘制区域轮廓，帮助用户理解区域信息。

    参数:
    - name (str): 区域的名字
    """
    CommandEvent = {"type": "map",
                    "operation":"draw-boundary",
                    "data": name}
    json_str = json.dumps(CommandEvent)  # 转成 JSON 字符串
    await manager.send_message(json_str)
    print(json_str)
    return "在地图上绘制了{}".format(name)

draw_boundary_tool = FunctionTool(
    draw_boundary,
    name="draw_boundary",
    description="在用户可见的地图上绘制某个区域轮廓，帮助用户理解城市或区域信息。",
)