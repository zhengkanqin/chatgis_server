import json
import os
from GeoFile.Service.ToolService import read_file, geo_data_convert, attribute_query, buffer_query
from AgentTools.RAG import Query_GeoFile
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
from Agent.MultiAgent_Node import *


os.environ["LANGSMITH_TRACING"] = "true"
os.environ["LANGCHAIN_API_KEY"] = "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"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
os.environ["LANGCHAIN_PROJECT"] = "Multi-Agent"

#图配置-----------------------------------------------------------------------------------------------------------------------------------------------------
workflow = StateGraph(GIS_State)
#节点配置---------------------------------------------------------------------------------------------------------------------------------------------------
workflow.add_node("chat_start_node", chat_start)

workflow.add_node("plan_make_node", plan_make)

workflow.add_node("summary_operation_node", summary_operation)

workflow.add_node("map_operation_node", map_operation)
workflow.add_node("analysis_operation_node", analysis_operation)
workflow.add_node("search_operation_node", search_operation)

workflow.add_node("reflection_operation_node", reflection_operation)

workflow.add_node("human_branch_operation_node", human_branch_operation)
workflow.add_node("human_plan_operation_node", human_plan_operation)

workflow.add_node("map_tool_node", map_tool_node)
workflow.add_node("analysis_tool_node", analysis_tool_node)
workflow.add_node("search_tool_node", search_tool_node)
workflow.add_node("knowledge_tool_node", knowledge_tool_node)

workflow.set_entry_point("chat_start_node")
#边配置-------------------------------------------------------------------------------------------------------------------------------------------------------

workflow.add_conditional_edges("chat_start_node", condition_chat_start)
workflow.add_conditional_edges("plan_make_node", condition_plan_make)
workflow.add_conditional_edges("reflection_operation_node", condition_reflection)
workflow.add_conditional_edges("summary_operation_node", condition_summary)
workflow.add_conditional_edges("map_operation_node", condition_map)
workflow.add_conditional_edges("analysis_operation_node", condition_analysis)
workflow.add_conditional_edges("search_operation_node", condition_search)
workflow.add_conditional_edges("human_branch_operation_node", condition_human_branch)

workflow.add_edge("knowledge_tool_node","plan_make_node")
workflow.add_edge("human_plan_operation_node","plan_make_node")
workflow.add_edge("map_tool_node","map_operation_node")
workflow.add_edge("analysis_tool_node","analysis_operation_node")
workflow.add_edge("search_tool_node","search_operation_node")

#编译配置--------------------------------------------------------------------------------------------------------------------------------------------------------
AgentMemory = MemorySaver()
MultiAgent = workflow.compile(checkpointer=AgentMemory)


























