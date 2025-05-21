# backend/agent_config.py
import json
from autogen_agentchat.agents import AssistantAgent
from autogen_ext.models.openai import OpenAIChatCompletionClient
import FunctionCallList


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
    tools=[FunctionCallList.draw_city_tool],
    reflect_on_tool_use=True,
    model_client_stream=True,
)
