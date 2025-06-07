import json
import os

from GeoFile.Service.ToolService import read_file, attribute_query, buffer_query, shp_to_type
from GeoFile.Tools.BufferTool import create_buffer_png
from RAG import Query_GeoFile
from baidumaptools import map_reverse_geocode
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

from map import draw_boundary, draw_point, draw_line, draw_polygon, draw_circle, draw_label, draw_geojson, draw_image

os.environ["LANGSMITH_TRACING"] = "true"
os.environ["LANGCHAIN_API_KEY"] = "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"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
os.environ["LANGCHAIN_PROJECT"] = "my-task-agent"

with open('./config.json', 'r', encoding='utf-8') as configFile:
    system_config = json.load(configFile)


# tools=AnalysisTools

tools = [draw_boundary,draw_circle,draw_label,draw_geojson,draw_image,read_file,shp_to_type,attribute_query,buffer_query,buffer_query,Query_GeoFile,map_reverse_geocode]

llm = ChatOpenAI(model=system_config["对话大模型名称"], api_key=system_config["对话大模型密钥"],
                 base_url=system_config["对话大模型地址"], temperature=0.4).bind_tools(tools)


tool_node = ToolNode(tools)


def should_continue(state:GIS_State):
    messages = state["messages"]
    last_message = messages[-1]
    if last_message.tool_calls:
        return "tools"
    return END

def call_model(state:GIS_State):
    messages = state["messages"]
    if not any(isinstance(m, SystemMessage) for m in messages):
        messages.insert(0, SystemMessage(content=
"""
你是一个智能助手
"""))
    response = llm.invoke(messages)
    return {"messages": [response],"sender":"123"}

workflow = StateGraph(GIS_State)
workflow.add_node("agent", call_model)
workflow.add_node("tools", tool_node)

workflow.set_entry_point("agent")

workflow.add_conditional_edges("agent", should_continue)

#tools -> Agent
workflow.add_edge("tools", "agent")

memory = MemorySaver()

GeoTestAgent = workflow.compile(checkpointer=memory)





