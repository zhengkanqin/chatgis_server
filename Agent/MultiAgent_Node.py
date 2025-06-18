#环境配置
import json
import re
from Agent.MultiAgent_Prompt import prompt_chat_start, prompt_plan, prompt_maps, prompt_analysis, prompt_searches,prompt_summary, prompt_reflection
from Agent.MultiAgent_func import sender_info
from AgentTools.baidumaptools import map_directions, map_reverse_geocode
from GeoFile.Service.ToolService import read_file, geo_data_convert, attribute_query, buffer_query, buffer_create,spatial_query
from AgentTools.RAG import Query_GeoFile, Query_Knowledge
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import END
from langgraph.prebuilt import ToolNode
from langgraph.types import interrupt
from Agent.GIS_State import GIS_State
from AgentTools.map import draw_boundary, draw_circle, draw_image, draw_geojson




with open('./config.json', 'r', encoding='utf-8') as configFile:
    system_config = json.load(configFile)


#大语言模型配置--------------------------------------------------------------------------------------------------------------------------------------------
map_tools = [
    draw_boundary,  #绘制边界
    draw_circle,    #绘制圆
    draw_image,     #绘制瓦片
    draw_geojson,   #绘制GeoJSON的点线面
    geo_data_convert,
]
map_llm = ChatOpenAI(model=system_config["对话大模型名称"], api_key=system_config["对话大模型密钥"],base_url=system_config["对话大模型地址"], temperature=0.4).bind_tools(map_tools)
#------------------------------------------------------------
analysis_tools = [
    read_file,
    geo_data_convert,
    # attribute_query,
    spatial_query,
    map_directions,
    map_reverse_geocode
]
analysis_llm = ChatOpenAI(model=system_config["对话大模型名称"], api_key=system_config["对话大模型密钥"],base_url=system_config["对话大模型地址"], temperature=0.4).bind_tools(analysis_tools)
#------------------------------------------------------------
search_tools = [Query_GeoFile]
search_llm = ChatOpenAI(model=system_config["对话大模型名称"], api_key=system_config["对话大模型密钥"],base_url=system_config["对话大模型地址"], temperature=0.4).bind_tools(search_tools)
#------------------------------------------------------------
knowledge_tools = [Query_Knowledge]
knowledge_llm = ChatOpenAI(model=system_config["对话大模型名称"], api_key=system_config["对话大模型密钥"],base_url=system_config["对话大模型地址"], temperature=0.1).bind_tools(knowledge_tools)
#------------------------------------------------------------
no_tool_llm = ChatOpenAI(model=system_config["对话大模型名称"], api_key=system_config["对话大模型密钥"],base_url=system_config["对话大模型地址"], temperature=0.2)


#函数配置----------------------------------------------------------------------------------------------------------------------------------------------------
def chat_start(state:GIS_State):
    messages = state["messages"]
    messages = [m for m in state["messages"] if not isinstance(m, SystemMessage)]
    messages.insert(0, SystemMessage(content=prompt_chat_start))
    response = no_tool_llm.invoke(messages)
    return {"messages": [response]}

def plan_make(state:GIS_State):
    messages = state["messages"]
    messages = [m for m in state["messages"] if not isinstance(m, SystemMessage)]
    messages.insert(0, SystemMessage(content=prompt_plan))
    response = knowledge_llm.invoke(messages)
    if sender_info(response.content):
        return {
                "messages":[response],
                "temp_messages": {"__clear__": True},  #清空临时消息---------------------
                "sender":sender_info(response.content)
                }
    else:
        return {"messages": [response]}

def map_operation(state:GIS_State):
    messages = state["temp_messages"]
    messages = [m for m in state["temp_messages"] if not isinstance(m, SystemMessage)]
    messages = state["act_messages"] + messages
    messages.insert(0, SystemMessage(content=prompt_maps))
    response = map_llm.invoke(messages)
    if response.content == messages[-1].content and "[$fail]" not in messages[-1].content and "[$end]" not in messages[-1].content:
        return {"messages":[HumanMessage(content=f"[$fail]{messages[-1].content}[$fail]")],"temp_messages": [HumanMessage(content=f"[$fail]{messages[-1].content}[$fail]")]} #防止陷入死循环
    if "[$end][$end]" in response.content or "[$fail]" in response.content:
        return {"messages":[response],"temp_messages": [response],"act_messages": [response]}
    return {"temp_messages": [response]}

def analysis_operation(state:GIS_State):
    messages = state["temp_messages"]
    messages = [m for m in state["temp_messages"] if not isinstance(m, SystemMessage)]
    messages = state["act_messages"] + messages
    messages.insert(0, SystemMessage(content=prompt_analysis))
    response = analysis_llm.invoke(messages)
    if response.content == messages[-1].content and "[$fail]" not in messages[-1].content and "[$end]" not in messages[-1].content:
        return {"messages":[HumanMessage(content=f"[$fail]{messages[-1].content}[$fail]")],"temp_messages": [HumanMessage(content=f"[$fail]{messages[-1].content}[$fail]")]} #防止陷入死循环
    if "[$end][$end]" in response.content or "[$fail]" in response.content:
        return {"messages":[response],"temp_messages": [response],"act_messages": [response]}
    return {"temp_messages": [response]}

def search_operation(state:GIS_State):
    messages = state["temp_messages"]
    messages = [m for m in state["temp_messages"] if not isinstance(m, SystemMessage)]
    messages = state["act_messages"] + messages
    messages.insert(0, SystemMessage(content=prompt_searches))
    response = search_llm.invoke(messages)
    if response.content == messages[-1].content and "[$fail]" not in messages[-1].content and "[$end]" not in messages[-1].content:
        return {"messages":[HumanMessage(content=f"[$fail]{messages[-1].content}[$fail]")],"temp_messages": [HumanMessage(content=f"[$fail]{messages[-1].content}[$fail]")]} #防止陷入死循环
    if "[$end][$end]" in response.content or "[$fail]" in response.content:
        print("准备提交")
        return {"messages":[response],"temp_messages": [response],"act_messages": [response]}
    return {"temp_messages": [response]}

def summary_operation(state:GIS_State):
    messages = state["messages"]
    messages = [m for m in state["messages"] if not isinstance(m, SystemMessage)]
    messages.append(SystemMessage(content=prompt_summary))  #尝试直接放在最后-----------------------------
    response = no_tool_llm.invoke(messages)
    return {"temp_messages": [response]}

def reflection_operation(state:GIS_State):
    messages = state["messages"]
    messages = [m for m in state["messages"] if not isinstance(m, SystemMessage)]
    messages.append(SystemMessage(content=prompt_reflection))
    response = no_tool_llm.invoke(messages)
    return {"messages": [response]}

def human_branch_operation(state:GIS_State):
    query_message = state["temp_messages"][-1]
    match=re.search(r'\[\$help](.*?)\[\$help]', query_message.content)
    if match:
        human_response = interrupt({"query": match.group(1)})
        response = human_response["messages"]
    else:
        response = HumanMessage(content="未能正常触发请求，请检查请求结构")
    return {"temp_messages": response}

def human_plan_operation(state:GIS_State):
    query_message = state["messages"][-1]
    match=re.search(r'\[\$help](.*?)\[\$help]', query_message.content)
    if match:
        human_response = interrupt({"query": match.group(1)})
        response = human_response["messages"]

    else:
        response = HumanMessage(content="未能正常触发请求，请检查请求结构")
    return {"messages": response}


map_tool_node = ToolNode(map_tools,messages_key="temp_messages") #地图操作工具节点集
analysis_tool_node = ToolNode(analysis_tools,messages_key="temp_messages") #地理文件分析操作工具节点集
search_tool_node = ToolNode(search_tools,messages_key="temp_messages") #地理文件及相关知识搜索工具节点集
knowledge_tool_node = ToolNode(knowledge_tools) #知识搜索节点

#边配置-------------------------------------------------------------------------------------------------------------------

#END
#chat_start_node
#plan_make_mode
#summary_operation_node
#map_operation_node
#analysis_operation_node
#search_operation_node
#reflection_operation_node
#human_branch_operation_node
#human_plan_operation_node
#map_tool_node
#analysis_tool_node
#search_tool_node
#knowledge_tool_node

def condition_chat_start(state:GIS_State):
    last_messages = state["messages"][-1]
    if "[$end][$end]" in last_messages.content:
        return END
    else:
        return "plan_make_node"

def condition_plan_make(state:GIS_State):
    last_messages = state["messages"][-1]
    if last_messages.tool_calls:
        return "knowledge_tool_node"
    if "[$help]" in last_messages.content:
        return "human_plan_operation_node"
    if "[$end]" in last_messages.content:
        return "reflection_operation_node"
    if "[$sender]" in last_messages.content:
        return "summary_operation_node"
    else:
        print("系统异常！轮询失败！")
        return "plan_make_node"

def condition_reflection(state:GIS_State):
    last_messages = state["messages"][-1]
    if "[$fail]" in last_messages.content:
        return "plan_make_node"
    else:
        return END

def condition_summary(state:GIS_State):
    sender = state["sender"]
    if "搜索" in sender:
        return "search_operation_node"
    if "分析" in sender:
        return "analysis_operation_node"
    if "图层" in sender:
        return "map_operation_node"
    else:
        print("系统异常！摘要分发失败！")
        return "plan_make_node"

def condition_search(state:GIS_State):
    last_messages = state["temp_messages"][-1]
    if last_messages.tool_calls:
        return "search_tool_node"
    if "[$help]" in last_messages.content:
        return "human_branch_operation_node"
    if "[$end]" in last_messages.content:
        return "plan_make_node"
    if "[$fail]" in last_messages.content:
        return "plan_make_node"
    else:
        print("系统异常！查询提交失败！")
        return "search_operation_node"

def condition_analysis(state:GIS_State):
    last_messages = state["temp_messages"][-1]
    if last_messages.tool_calls:
        return "analysis_tool_node"
    if "[$help]" in last_messages.content:
        return "human_branch_operation_node"
    if "[$end]" in last_messages.content:
        return "plan_make_node"
    if "[$fail]" in last_messages.content:
        return "plan_make_node"
    else:
        print("系统异常！分析提交失败！")
        return "analysis_operation_node"

def condition_map(state:GIS_State):
    last_messages = state["temp_messages"][-1]
    if last_messages.tool_calls:
        return "map_tool_node"
    if "[$help]" in last_messages.content:
        return "human_branch_operation_node"
    if "[$end]" in last_messages.content:
        return "plan_make_node"
    if "[$fail]" in last_messages.content:
        return "plan_make_node"
    else:
        print("系统异常！地图操作提交失败！")
        return "map_operation_node"

def condition_human_branch(state:GIS_State):
    sender = state["sender"]
    if "搜索" in sender:
        return "search_operation_node"
    if "分析" in sender:
        return "analysis_operation_node"
    if "图层" in sender:
        return "map_operation_node"
    else:
        print("系统异常！轮询失败！")
        return END




































