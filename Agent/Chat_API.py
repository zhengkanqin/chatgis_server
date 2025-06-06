from typing import AsyncGenerator, List, Optional
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langgraph.types import Command

from Agent.GIS_State import Layer, GIS_State, create_default_state
from Agent.Agent_Main import Agent_Main, workflow
import json
import base64
import requests

from Agent.GeoAgent import GeoTestAgent

# 模块级变量，用于存储中断状态
is_interrupted = False
interrupt_query = ""
# workAgent = Agent_Main
workAgent = GeoTestAgent

UserLayers = []



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


# --------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------
# -------------------------------------------具体逻辑------------------------------------------
# --------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------

async def event_generator(q: str,files: Optional[List[str]] = None,layers: Optional[List[Layer]] = None,mapInfo: Optional[str] = None,) -> AsyncGenerator[str, None]:
    global is_interrupted, interrupt_query,UserLayers

    # 如果mapInfo不为None，则读取本地图片文件，编码为base64，构造HumanMessage
    map_message = None
    if mapInfo:
        try:
            with open(mapInfo, 'rb') as f:
                img_bytes = f.read()
            img_base64 = base64.b64encode(img_bytes).decode('utf-8')
            data_url = f"data:image/png;base64,{img_base64}"
            map_message = HumanMessage(content=[{"type": "image_url", "image_url": {"url": data_url}}])
        except Exception as e:
            map_message = HumanMessage(content=f"地图文件读取失败：{mapInfo}，错误：{str(e)}")
    
    # 构造messages列表
    messages = []
    # 添加用户查询消息
    messages.append(HumanMessage(content=q))
    # 处理文件消息，添加文件URL到消息列表
    if files:
        file_messages = []
        for file_url in files:
            try:
                # 读取本地文件内容
                with open(file_url, 'rb') as f:
                    img_bytes = f.read()
                # 编码为base64
                img_base64 = base64.b64encode(img_bytes).decode('utf-8')
                # 构造data url
                data_url = f"data:image/png;base64,{img_base64}"
                # 构造消息
                file_messages.append(HumanMessage(content=[{"type": "image_url", "image_url": {"url": data_url}}]))
            except Exception as e:
                # 读取或编码失败时，插入错误消息
                file_messages.append(HumanMessage(content=f"文件读取失败：{file_url}，错误：{str(e)}"))
        # 将文件消息添加到消息列表的开头
        messages = file_messages + messages
    UserLayers = []
    if layers:
        UserLayers = layers
        print("有图层传入")

        short_layers = []
        long_layers = []
        short_layers_messages = []
        long_layers_messages = []

        for layer in layers:
            if len(json.dumps(layer)) < 1000:
                short_layers.append(layer)
            else:
                long_layers.append(layer)

        for short_layer in short_layers:
            msg = f"图层名：{short_layer['name']}，图层类型：{short_layer['type']}，图层数据：{short_layer['data']}"
            short_layers_messages.append(msg)
            print(msg)

        for long_layer in long_layers:
            msg = f"图层名：{long_layer['name']}，图层类型：{long_layer['type']}，图层数据：数据过大，不显示。"
            long_layers_messages.append(msg)
            print(msg)

        # 拼接所有图层信息
        all_layer_info = "\n".join(short_layers_messages + long_layers_messages)
        final_message = "用户指定了以下图层请求对话：\n" + all_layer_info
        final_layer_message = HumanMessage(content=final_message)
        messages = [final_layer_message] + messages

    # 如果处于中断状态，直接使用Command恢复会话
    if is_interrupted:
        try:
            is_interrupted = False
            human_command = Command(resume={"messages": messages,
                                            "layers":layers,
                                            "mapinfo": map_message
                                            })
            async for update in workAgent.astream(
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
        # 使用 LangGraph 的异步流式方法
        async for update in workAgent.astream(
                input={"messages": messages,
                        "layers":layers,
                        "mapinfo": map_message
                        },
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


