import asyncio
import json

from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

with open('config.json', 'r', encoding='utf-8') as configFile:
    config = json.load(configFile)

llm = ChatOpenAI(model=config["深度思考模型名称"], api_key=config["深度思考模型密钥"],
                 base_url=config["深度思考模型地址"], temperature=0.4)

async def test():
    abc = llm.invoke([HumanMessage(content="15+2=?")])
    print(abc)

if __name__ == "__main__":
    asyncio.run(test())