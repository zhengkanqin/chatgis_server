#环境配置
import json
import os
import re
from Agent.MultiAgent_Prompt import prompt_chat_start, prompt_plan, prompt_maps, prompt_analysis, prompt_searches, \
    prompt_summary, prompt_reflection
from GeoFile.Service.ToolService import read_file, to_geojson, attribute_query, buffer_query
from RAG import Query_GeoFile, Query_Knowledge
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
import MultiAgent_Prompt
os.environ["LANGSMITH_TRACING"] = "true"
os.environ["LANGCHAIN_API_KEY"] = "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"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
os.environ["LANGCHAIN_PROJECT"] = "Multi-Agent"

with open('./config.json', 'r', encoding='utf-8') as configFile:
    system_config = json.load(configFile)


#大语言模型配置--------------------------------------------------------------------------------------------------------------------------------------------
map_tools = [

]
map_llm = ChatOpenAI(model=system_config["对话大模型名称"], api_key=system_config["对话大模型密钥"],base_url=system_config["对话大模型地址"], temperature=0.4).bind_tools(map_tools)

analysis_tools = [

]
analysis_llm = ChatOpenAI(model=system_config["对话大模型名称"], api_key=system_config["对话大模型密钥"],base_url=system_config["对话大模型地址"], temperature=0.4).bind_tools(analysis_tools)

search_tools = [Query_GeoFile]
search_llm = ChatOpenAI(model=system_config["对话大模型名称"], api_key=system_config["对话大模型密钥"],base_url=system_config["对话大模型地址"], temperature=0.4).bind_tools(search_tools)
knowledge_tools = [Query_Knowledge]
knowlegde_llm = ChatOpenAI(model=system_config["对话大模型名称"], api_key=system_config["对话大模型密钥"],base_url=system_config["对话大模型地址"], temperature=0.4).bind_tools(knowledge_tools)
no_tool_llm = ChatOpenAI(model=system_config["对话大模型名称"], api_key=system_config["对话大模型密钥"],base_url=system_config["对话大模型地址"], temperature=0.4)


#函数配置----------------------------------------------------------------------------------------------------------------------------------------------------
def chat_start(state:GIS_State):
    messages = state["messages"]
    if not any(isinstance(m, SystemMessage) for m in messages):
        messages.insert(0, SystemMessage(content=prompt_chat_start))
    response = no_tool_llm.invoke(messages)
    return {"messages": [response]}

def plan_make(state:GIS_State):
    messages = state["messages"]
    if not any(isinstance(m, SystemMessage) for m in messages):
        messages.insert(0, SystemMessage(content=prompt_plan))
    response = knowlegde_llm.invoke(messages)
    return {"messages": [response]}

def map_operation(state:GIS_State):
    messages = state["temp_messages"]
    if not any(isinstance(m, SystemMessage) for m in messages):
        messages.insert(0, SystemMessage(content=prompt_maps))
    response = map_llm.invoke(messages)
    return {"temp_messages": [response]}

def analysis_operation(state:GIS_State):
    messages = state["temp_messages"]
    if not any(isinstance(m, SystemMessage) for m in messages):
        messages.insert(0, SystemMessage(content=prompt_analysis))
    response = analysis_llm.invoke(messages)
    return {"temp_messages": [response]}

def search_operation(state:GIS_State):
    messages = state["temp_messages"]
    if not any(isinstance(m, SystemMessage) for m in messages):
        messages.insert(0, SystemMessage(content=prompt_searches))
    response = search_llm.invoke(messages)
    return {"temp_messages": [response]}

def summary_operation(state:GIS_State):
    messages = state["messages"]
    if not any(isinstance(m, SystemMessage) for m in messages):
        messages.insert(0, SystemMessage(content=prompt_summary))
    response = no_tool_llm.invoke(messages)
    return {"temp_messages": [response]}

def reflection_operation(state:GIS_State):
    messages = state["messages"]
    if not any(isinstance(m, SystemMessage) for m in messages):
        messages.insert(0, SystemMessage(content=prompt_reflection))
    response = no_tool_llm.invoke(messages)
    return {"messages": [response]}

def human_branch_operation(state:GIS_State):
    query_message = state["temp_messages"][-1]
    match=re.search(r'\[\$help](.*?)\[\$help]', query_message.content)
    if match:
        human_response = interrupt({"query": match.group(1)})
        response = HumanMessage(content=human_response)
    else:
        response = HumanMessage(content="未能正常触发请求，请检查请求结构")
    return {"temp_messages": [response]}

def human_plan_operation(state:GIS_State):
    query_message = state["messages"][-1]
    match=re.search(r'\[\$help](.*?)\[\$help]', query_message.content)
    if match:
        human_response = interrupt({"query": match.group(1)})
        response = HumanMessage(content=human_response)
    else:
        response = HumanMessage(content="未能正常触发请求，请检查请求结构")
    return {"messages": [response]}


map_tool_node = ToolNode(map_tools,messages_key="temp_messages") #地图操作工具节点集
analysis_tool_node = ToolNode(analysis_tools,messages_key="temp_messages") #地理文件分析操作工具节点集
search_tool_node = ToolNode(search_tools,messages_key="temp_messages") #地理文件及相关知识搜索工具节点集
knowledge_tool_node = ToolNode(knowledge_tools,messages_key="messages") #知识搜索节点

