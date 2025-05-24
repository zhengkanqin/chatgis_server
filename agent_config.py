# backend/agent_config.py
import json
from autogen_agentchat.agents import AssistantAgent
from autogen_ext.models.openai import OpenAIChatCompletionClient
from FunctionCall_List import draw_boundary_tool
from GeoFile.Service.DataInputService import read_tool

with open('config.json', 'r', encoding='utf-8') as configFile:
    config = json.load(configFile)


model_info = {
    "name": "deepseek-chat",
    "parameters": {"max_tokens": 2048, "temperature": 0.4, "top_p": 0.9},
    "family": "gpt-4o",
    "vision": True,
    "json_output": True,
    "function_calling": True,
    "structured_output": True,
}

model_client = OpenAIChatCompletionClient(
    model=config["对话大模型名称"],
    base_url=config["对话大模型地址"],
    api_key=config["对话大模型密钥"],
    model_info=model_info,
)



agent = AssistantAgent(
    name="assistant",
    model_client=model_client,
    system_message="你的名字是GIS助手，你需要提供地理信息相关的服务，并尽可能的让用户详细理解。如果用户需要介绍地方，能绘制地图则调用绘制地图的工具",
    tools=[draw_boundary_tool],
    reflect_on_tool_use=True,
    model_client_stream=True,
)

geoFileAgent = AssistantAgent(
    name="GeoFileReader",
    model_client=model_client,
    system_message="你是文件理解器，你会收到一条条文件地址，你的工作是使用文件阅读工具，结合地理信息领域的知识，精确且结构化的描述地理数据，一定要陈述其绝对文件路径。遇到经纬度的范围时，可以适当补充其大致位置信息，使用markdown格式，不要有多余的输出。",
    tools=[read_tool],
    reflect_on_tool_use=True,
    model_client_stream=True,
)
