import json
import os
from GeoFile.Service.ToolService import read_file, to_geojson, attribute_query, buffer_query
from RAG import Query_GeoFile
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
from MultiAgent_Node import *




#图配置-----------------------------------------------------------------------------------------------------------------------------------------------------
workflow = StateGraph(GIS_State)
#节点配置---------------------------------------------------------------------------------------------------------------------------------------------------
workflow.add_node("chat_start_node", chat_start)

workflow.add_node("plan_make_mode", plan_make)

workflow.add_node("summary_operation", summary_operation)

workflow.add_node("map_operation", map_operation)
workflow.add_node("analysis_operation", analysis_operation)
workflow.add_node("search_operation", search_operation)

workflow.add_node("reflection_operation", reflection_operation)

workflow.add_node("human_branch_operation", human_branch_operation)
workflow.add_node("human_plan_operation", human_plan_operation)

workflow.add_node("map_tool_node", map_tool_node)
workflow.add_node("analysis_tool_node", analysis_tool_node)
workflow.add_node("search_tool_node", search_tool_node)
workflow.add_node("knowledge_tool_node", knowledge_tool_node)

workflow.set_entry_point("chat_start_node")
#边配置-------------------------------------------------------------------------------------------------------------------------------------------------------







