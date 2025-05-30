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

os.environ["LANGSMITH_TRACING"] = "true"
os.environ["LANGCHAIN_API_KEY"] = "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"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
os.environ["LANGCHAIN_PROJECT"] = "my-task-agent"

with open('./config.json', 'r', encoding='utf-8') as configFile:
    system_config = json.load(configFile)


@tool()
def search(query: str):
    """模拟一个搜索工具"""
    if "上海" in query.lower() or "Shanghai" in query.lower():
        return "现在30度，有雾."
    return "现在是35度，阳光明媚。"


@tool()
def draw_boundary(name: str):
    """
    在地图上绘制区域边界

    参数：
        name（str）：区域名
    """
    print("adoihawiodhoadhoiahdo1")
    return "绘制成功"


tools = [search, draw_boundary]

llm = ChatOpenAI(model=system_config["对话大模型名称"], api_key=system_config["对话大模型密钥"],
                 base_url=system_config["对话大模型地址"], temperature=0.4).bind_tools(tools)

# res = llm.invoke([HumanMessage(content="你好")])

tool_node = ToolNode(tools)


def should_continue(state: MessagesState):
    messages = state["messages"]
    last_message = messages[-1]
    if last_message.tool_calls:
        return "tools"
    return END


def call_model(state: MessagesState):
    messages = state["messages"]
    if not any(isinstance(m, SystemMessage) for m in messages):
        # 添加系统提示词，仅当未提供时添加（避免重复）
        messages.insert(0, SystemMessage(content="""
你是一个智能助手
"""
                                         ))
    response = llm.invoke(messages)
    # human_response = interrupt({"query": "乐乐乐"})
    return {"messages": response}


workflow = StateGraph(MessagesState)
workflow.add_node("agent", call_model)
workflow.add_node("tools", tool_node)

workflow.set_entry_point("agent")

workflow.add_conditional_edges("agent", should_continue)

#tools -> Agent
workflow.add_edge("tools", "agent")

memory = MemorySaver()

Test_Agent = workflow.compile(checkpointer=memory)

# final_state = Test_Agent.invoke(
#     input={"messages":[HumanMessage(content="查询上海天气")]},
#     config={"configurable": {"thread_id": 42}}
# )
# print(final_state["messages"][-1].content)
# print("--------------------------")
# final_state = Test_Agent.invoke(
#     input={"messages":[HumanMessage(content="在地图上绘制枝江市边界")]},
#     config={"configurable": {"thread_id": 42}}
# )
# print(final_state["messages"][-1].content)
#
#
# #本测试案例用于测试简单的调用工具智能体环节。

