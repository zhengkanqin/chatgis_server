import json
import os
import uuid

from GeoFile.Service.ToolService import read_file, attribute_query
from AgentTools.RAG import Query_GeoFile
from AgentTools.baidumaptools import map_reverse_geocode
from connection_manager import manager
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph, MessagesState
from langgraph.prebuilt import ToolNode
from langgraph.types import Command, interrupt
from Agent.GIS_State import Layer, GIS_State
from typing import List

with open('./config.json', 'r', encoding='utf-8') as configFile:
    system_config = json.load(configFile)

tools = [read_file,attribute_query,Query_GeoFile,map_reverse_geocode]

llm = ChatOpenAI(model=system_config["地理数据解析模型名称"], api_key=system_config["地理数据解析模型密钥"],
                 base_url=system_config["地理数据解析模型地址"], temperature=0.1).bind_tools(tools)


tool_node = ToolNode(tools)


def should_continue(state:MessagesState):
    messages = state["messages"]
    last_message = messages[-1]
    if last_message.tool_calls:
        return "tools"
    return END

def call_model(state:MessagesState):
    messages = state["messages"]
    if not any(isinstance(m, SystemMessage) for m in messages):
        messages.insert(0, SystemMessage(content=
"""
你是一个地理文件阅读助手。你的职责是请接收文件地址，调用若干你感兴趣的文件阅读工具，输出markdown-it风格的文件内容详细的元数据描述，必须要包含文件路径，把属性字段部分做成表格。
注意事项：不要输出无关和虚假内容,以及任何提示性信息，只关注内容本身，允许根据内容信息进行推理和补充描述。
"""))
    response = llm.invoke(messages)
    return {"messages": [response]}

workflow = StateGraph(MessagesState)
workflow.add_node("file_read_agent", call_model)
workflow.add_node("tools", tool_node)

workflow.set_entry_point("file_read_agent")

workflow.add_conditional_edges("file_read_agent", should_continue)

#tools -> Agent
workflow.add_edge("tools", "file_read_agent")

memory = MemorySaver()

FileReadAgent = workflow.compile(checkpointer=memory)


async def read_file(path:str):
    thread_id = str(uuid.uuid4())
    state = MessagesState(messages=[HumanMessage(content=f"请读取并分析文件：{path}")])
    result = await FileReadAgent.ainvoke(state,config={"configurable": {"thread_id": thread_id}})
    # 获取最后一条消息的内容
    last_message = result["messages"][-1]
    return last_message.content
