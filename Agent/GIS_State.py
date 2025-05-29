from typing import TypedDict, List, NotRequired
from langchain_core.messages import BaseMessage

class MyState(TypedDict):
    messages: List[BaseMessage]  # 保留默认消息结构
    layers: List[str]  # 位置信息
    map:str
def create_default_state() -> MyState:
    """创建默认状态"""
    return {
        "messages": [],
        "layers": [],
        "map":""
    }
