from typing import TypedDict, List, NotRequired, Annotated, Any, Union, Dict
from langchain_core.messages import BaseMessage, HumanMessage
from operator import add

def add_or_clear(old: List[Any], new: Union[List[Any], Dict[str, Any]]) -> List[Any]:
    if isinstance(new, dict):
        if new.get("__clear__") is True:
            print("-------------------------------------------------------")
            print(new.get("add",[]))
            return new.get("add", [])
    return old + new


class Layer(TypedDict):
    name:str
    type:str
    data:NotRequired[dict]

class GIS_State(TypedDict):
    messages: Annotated[List[BaseMessage], add]
    temp_messages: Annotated[List[BaseMessage], add_or_clear]
    sender:str
    mapMessage:HumanMessage
    layers:List[Layer]
def create_default_state() -> GIS_State:
    """创建默认状态"""
    return {
        "messages": [],
        "temp_messages": [],
        "sender": "",
        "mapMessage": HumanMessage(content="无图像信息"),
        "layers": []
    }



