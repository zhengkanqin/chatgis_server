import os
from autogen.agentchat import AssistantAgent, UserProxyAgent
from autogen.retrieve_utils import LocalVectorStore
from autogen.tools import Tool

# 初始化向量存储
vector_store = LocalVectorStore(
    doc_folder="./docs",  # 文档路径
    embedding_model="all-MiniLM-L6-v2"  # 嵌入模型
)

# 定义检索工具
def retrieve_documents(query):
    return vector_store.retrieve(query, top_k=3)

retrieval_tool = Tool(
    name="document_retriever",
    func=retrieve_documents,
    description="根据查询从文档中检索相关内容。"
)

# 创建助手代理
assistant = AssistantAgent(
    name="assistant",
    llm_config={"model": "gpt-4"},
    tools=[retrieval_tool]
)

# 创建用户代理
user_proxy = UserProxyAgent(
    name="user_proxy",
    human_input_mode="ALWAYS"
)

# 启动对话
user_proxy.initiate_chat(
    assistant,
    message="请解释气候变化对经济的影响。"
)
