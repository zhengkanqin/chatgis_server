from typing import AsyncGenerator, List, Optional
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langgraph.types import Command

from Agent.GIS_State import Layer, GIS_State, create_default_state
from Agent.Agent_Main import Agent_Main
import json


# 模块级变量，用于存储中断状态
is_interrupted = False
interrupt_query = ""

state:GIS_State = create_default_state()

def safe_json_serialize(obj):
    """安全地将对象转换为可JSON序列化的格式"""
    if hasattr(obj, '__dict__'):
        return {key: safe_json_serialize(value) for key, value in obj.__dict__.items()}
    elif isinstance(obj, (list, tuple)):
        return [safe_json_serialize(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: safe_json_serialize(value) for key, value in obj.items()}
    elif hasattr(obj, '__str__'):
        return str(obj)
    return obj


async def process_messages(update: dict) -> AsyncGenerator[str, None]:
    """处理消息的通用逻辑"""
    print(update)
    if "agent" in update and "messages" in update["agent"]:
        messages = update["agent"]["messages"]
        # 只处理 AI 消息
        for message in messages:
            # 跳过系统消息和人类消息
            if isinstance(message, (SystemMessage, HumanMessage)):
                continue
                
            if hasattr(message, "content") and message.content:
                response = {
                    "type": "message",
                    "content": message.content
                }

                yield f"data: {json.dumps(response, ensure_ascii=False)}\n\n"
            elif hasattr(message, "tool_calls"):
                for tool_call in message.tool_calls:
                    response = {
                        "type": "tool_start",
                        "content": f"正在执行{tool_call['name']}..."
                    }

                    yield f"data: {json.dumps(response, ensure_ascii=False)}\n\n"
    elif "tools" in update and "messages" in update["tools"]:
        for tool_message in update["tools"]["messages"]:
            if hasattr(tool_message, "content"):
                response = {
                    "type": "tool_result",
                    "content": tool_message.content
                }

                yield f"data: {json.dumps(response, ensure_ascii=False)}\n\n"

#--------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------
#-------------------------------------------具体逻辑------------------------------------------
#--------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------

async def event_generator(
    q: str,
    files: Optional[List[str]] = None,
    layers: Optional[List[Layer]] = None,
    mapInfo: Optional[str] = None,
) -> AsyncGenerator[str, None]:
    global is_interrupted, interrupt_query
    # 如果处于中断状态，直接使用Command恢复会话
    if is_interrupted:
        try:
            is_interrupted = False
            human_command = Command(resume={"data": q})
            async for update in Agent_Main.astream(
                    input=human_command,
                    config={"configurable": {"thread_id": 42}},
                    stream_mode="updates"
            ):
                async for message in process_messages(update):
                    yield message
        except Exception as e:
            error_message = {
                "type": "error",
                "content": str(e)
            }
            yield f"data: {json.dumps(error_message, ensure_ascii=False)}\n\n"
        else:
            end_message = {
                "type": "end",
                "content": "对话结束"
            }
            yield f"data: {json.dumps(end_message, ensure_ascii=False)}\n\n"
        return
    
    # 正常对话流程
    try:
        messages = []
        
        # 添加用户查询消息
        messages.append(HumanMessage(content=q))
        
        # 处理文件消息，添加文件URL到消息列表
        if files:
            file_messages = []
            for file_url in files:
                file_messages.append(HumanMessage(content=f"文件URL：{file_url}"))
            # 将文件消息添加到消息列表的开头
            messages = file_messages + messages

        # 使用 LangGraph 的异步流式方法
        async for update in Agent_Main.astream(
                input={"messages": messages},
                config={"configurable": {"thread_id": 42}},
                stream_mode="updates"
        ):
            # 检查是否是interrupt类型
            if "__interrupt__" in update:
                interrupt_obj = update["__interrupt__"][0]
                if hasattr(interrupt_obj, 'value') and isinstance(interrupt_obj.value, dict):
                    interrupt_query = interrupt_obj.value.get('query', '')
                    is_interrupted = True
                    response = {
                        "type": "interrupt",
                        "content": interrupt_query
                    }
                    yield f"data: {json.dumps(response, ensure_ascii=False)}\n\n"
                    return
            
            # 处理普通消息
            async for message in process_messages(update):
                yield message
                
    except Exception as e:
        error_message = {
            "type": "error",
            "content": str(e)
        }
        yield f"data: {json.dumps(error_message, ensure_ascii=False)}\n\n"
    else:
        end_message = {
            "type": "end",
            "content": "对话结束"
        }
        yield f"data: {json.dumps(end_message, ensure_ascii=False)}\n\n"