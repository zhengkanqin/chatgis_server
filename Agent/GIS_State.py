from typing import TypedDict, List, NotRequired, Annotated
from langchain_core.messages import BaseMessage
from operator import add

class Layer(TypedDict):
    name:str
    type:str
    data:NotRequired[dict]

class GIS_State(TypedDict):
    messages: Annotated[List[BaseMessage], add]
    temp_messages: Annotated[List[BaseMessage], add]
    sender:str
    mapInfo:str
    layers:List[Layer]

def create_default_state() -> GIS_State:
    """创建默认状态"""
    return {
        "messages": [],
        "temp_messages": [],
        "sender": "",
        "mapInfo": "",
        "layers": []
    }

