from typing import TypedDict, List, NotRequired, Annotated, Any, Union, Dict
from langchain_core.messages import BaseMessage, HumanMessage
from operator import add

def add_or_clear(old: List[Any], new: Union[List[Any], Dict[str, Any]]) -> List[Any]:
    # 如果收到特殊标志，清空该字段
    if isinstance(new, dict) and new.get("__clear__") is True:
        return []
    # 否则执行正常追加
    return old + new


class Layer(TypedDict):
    name:str
    type:str
    data:NotRequired[dict]

class GIS_State(TypedDict):
    messages: Annotated[List[BaseMessage], add]
    temp_messages: Annotated[List[BaseMessage], add_or_clear]
    act_messages: Annotated[List[BaseMessage], add]
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
        "layers": [],
        "act_messages": [],
    }

class GISTask:
    description:str
    resource:str
    state:str
    sender:str
    feedback:str

class GISPlan:
    UserGoal:str
    TotalThinking:str
    SubTask:list[GISTask]

    def __init__(self,goal):
        self.UserGoal=goal
