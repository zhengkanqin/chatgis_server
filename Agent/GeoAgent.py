import json
import os
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

os.environ["LANGSMITH_TRACING"] = "true"
os.environ["LANGCHAIN_API_KEY"] = "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"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
os.environ["LANGCHAIN_PROJECT"] = "my-task-agent"

with open('./config.json', 'r', encoding='utf-8') as configFile:
    system_config = json.load(configFile)


llm_main = ChatOpenAI(model=system_config["对话大模型名称"], api_key=system_config["对话大模型密钥"],
                 base_url=system_config["对话大模型地址"], temperature=0.4)

















workflow = StateGraph(GIS_State)








memory = MemorySaver()
Agent_Main = workflow.compile(checkpointer=memory)