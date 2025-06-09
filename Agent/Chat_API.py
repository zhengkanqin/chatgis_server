from typing import AsyncGenerator, List, Optional
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langgraph.types import Command
import contextlib
from Agent.GIS_State import Layer, GIS_State, create_default_state
from Agent.Agent_Main import Agent_Main, workflow
import json
import base64
import requests
from Globals import UserLayers
from Agent.GeoAgent import GeoTestAgent
from Agent.MultiAgent import MultiAgent

# 模块级变量，用于存储中断状态
is_interrupted = False
interrupt_query = ""
# workAgent = Agent_Main
# workAgent = GeoTestAgent
workAgent = MultiAgent



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
    # 检查update中是否包含任何节点的消息
    for node_name, node_data in update.items():
        if isinstance(node_data, dict):
            # 检查messages字段
            if "messages" in node_data:
                messages = node_data["messages"]
                # 只处理 AI 消息
                for message in messages:
                    # 跳过系统消息和人类消息
                    if isinstance(message, (SystemMessage, HumanMessage)):
                        continue

                    if hasattr(message, "content") and message.content:
                        # 检查是否是工具调用结果
                        if hasattr(message, "tool_call_id"):
                            response = {
                                "type": "tool_result",
                                "content": message.content,
                                "sender": node_name
                            }
                        else:
                            response = {
                                "type": "message",
                                "content": message.content,
                                "sender": node_name
                            }

                        yield f"data: {json.dumps(response, ensure_ascii=False)}\n\n"
                    elif hasattr(message, "tool_calls"):
                        for tool_call in message.tool_calls:
                            response = {
                                "type": "tool_start",
                                "content": f"正在执行{tool_call['name']}...",
                                "sender": node_name
                            }

                            yield f"data: {json.dumps(response, ensure_ascii=False)}\n\n"
            
            # 检查temp_messages字段
            if "temp_messages" in node_data:
                temp_messages = node_data["temp_messages"]
                # 只处理 AI 消息
                for message in temp_messages:
                    # 跳过系统消息和人类消息
                    if isinstance(message, (SystemMessage, HumanMessage)):
                        continue

                    if hasattr(message, "content") and message.content:
                        # 检查是否是工具调用结果
                        if hasattr(message, "tool_call_id"):
                            response = {
                                "type": "tool_result",
                                "content": message.content,
                                "sender": node_name
                            }
                        else:
                            response = {
                                "type": "message",
                                "content": message.content,
                                "sender": node_name
                            }

                        yield f"data: {json.dumps(response, ensure_ascii=False)}\n\n"
                    elif hasattr(message, "tool_calls"):
                        for tool_call in message.tool_calls:
                            response = {
                                "type": "tool_start",
                                "content": f"正在执行{tool_call['name']}...",
                                "sender": node_name
                            }

                            yield f"data: {json.dumps(response, ensure_ascii=False)}\n\n"
        # 处理tools消息
        elif node_name == "tools" and "messages" in node_data:
            for tool_message in node_data["messages"]:
                if hasattr(tool_message, "content"):
                    response = {
                        "type": "tool_result",
                        "content": tool_message.content,
                        "sender": node_name
                    }

                    yield f"data: {json.dumps(response, ensure_ascii=False)}\n\n"


# --------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------
# -------------------------------------------具体逻辑------------------------------------------
# --------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------

async def event_generator(q: str, files: Optional[List[str]] = None,
                          layers: Optional[List[Layer]] = None,
                          mapInfo: Optional[str] = None) -> AsyncGenerator[str, None]:
    global is_interrupted, interrupt_query

    try:
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

        messages = [HumanMessage(content=q)]
        if files:
            file_messages = []
            for file_url in files:
                try:
                    with open(file_url, 'rb') as f:
                        img_bytes = f.read()
                    img_base64 = base64.b64encode(img_bytes).decode('utf-8')
                    data_url = f"data:image/png;base64,{img_base64}"
                    file_messages.append(HumanMessage(content=[{"type": "image_url", "image_url": {"url": data_url}}]))
                except Exception as e:
                    file_messages.append(HumanMessage(content=f"文件读取失败：{file_url}，错误：{str(e)}"))
            messages = file_messages + messages

        UserLayers.clear()
        if layers:
            UserLayers.extend(layers)
            short_layers, long_layers = [], []
            for layer in layers:
                if len(json.dumps(layer)) < 1000:
                    short_layers.append(layer)
                else:
                    long_layers.append(layer)

            layer_msgs = [
                f"图层名：{l['name']}，图层类型：{l['type']}，图层数据：{l.get('data', '')}"
                for l in short_layers
            ] + [
                f"图层名：{l['name']}，图层类型：{l['type']}，图层数据：数据过大，不显示。"
                for l in long_layers
            ]
            final_layer_message = HumanMessage(content="用户指定了以下图层请求对话：\n" + "\n".join(layer_msgs))
            messages = [final_layer_message] + messages

        input_data = {"messages": messages, "layers": layers, "mapinfo": map_message}
        if is_interrupted:
            is_interrupted = False
            input_data = Command(resume=input_data)

        async with contextlib.aclosing(workAgent.astream(
            input=input_data,
            config={"configurable": {"thread_id": 42}},
            stream_mode="updates"
        )) as astream:
            async for update in astream:
                try:
                    if "__interrupt__" in update:
                        interrupt_obj = update["__interrupt__"][0]
                        if hasattr(interrupt_obj, 'value') and isinstance(interrupt_obj.value, dict):
                            interrupt_query = interrupt_obj.value.get('query', '')
                            is_interrupted = True
                            yield f"data: {json.dumps({'type': 'interrupt', 'content': interrupt_query}, ensure_ascii=False)}\n\n"
                            return

                    async for message in process_messages(update):
                        yield message

                except GeneratorExit:
                    is_interrupted = True
                    return

    except GeneratorExit:
        is_interrupted = True
        return
    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'content': str(e)}, ensure_ascii=False)}\n\n"
        return


