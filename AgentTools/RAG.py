from Vector_DB_Memory import VectorDBMemory
from langchain_core.tools import tool

GeoFileMemory = VectorDBMemory(collection_name="GeoFile")
KnowledgeMemory = VectorDBMemory(collection_name="Knowledge")

@tool()
async def Query_GeoFile(keyword: str, num: int):
    """
       根据关键词查询所需地理文件

       Args:
           keyword: 查询关键词，可以用分号或者空格隔开以匹配多个关键词
           num: 返回最相似结果数量，默认5条
       """
    result = await GeoFileMemory.query(
        query=keyword,
        n_results=num
    )
    return {
        "results": [
            {
                "content": item.content
            }
            for item in result.results
        ]
    }

@tool()
async def Query_Knowledge(keyword: str, num: int):
    """
       根据关键词查询解决问题的知识经验，查不到或者不相关就算了，尝试自行解决问题。

       Args:
           keyword: 查询关键词，可以用分号或者空格隔开以匹配多个关键词
           num: 返回最相似结果数量，默认5条
       """
    result = await KnowledgeMemory.query(
        query=keyword,
        n_results=num
    )
    return {
        "knowledge": [
            {
                "content": item.content
            }
            for item in result.results
        ]
    }