# chat_handler.py
from typing import AsyncGenerator

from agent_config import agent,geoFileAgent
import json
async def handle_chat(q: str) -> str:
    result = await agent.run(task=q)  # 使用 await 调用 run 方法
    final_response = result.messages  # 获取最终响应消息
    print(result)
    return final_response


async def handle_readGeoFile(q: str) -> str:
    result = await geoFileAgent.run(task=q)  # 使用 await 调用 run 方法
    final_response = result.messages[-1].content  # 获取最终响应消息
    print(result)
    return final_response