# backend/main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from Agent.Chat_API import event_generator
from connection_manager import manager
import uvicorn
from Vector_DB_Memory import VectorDBMemory
from pydantic import BaseModel
from typing import Optional, Dict, Any, List, Union
import base64


app = FastAPI()

# CORS 中间件设置
app.add_middleware(CORSMiddleware,allow_origins=["*"], allow_credentials=True,allow_methods=["*"],allow_headers=["*"])


#-----------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------WS--------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------
# WebSocket 路由
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Response: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"WebSocket错误: {str(e)}")
#-----------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------RAG-------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------
GeoFileMemory = VectorDBMemory(collection_name="GeoFile")
class MemoryContent(BaseModel):
    content: str
    metadata: Optional[Dict[str, Any]] = None
    filepath: Optional[str] = None
class DeleteRequest(BaseModel):
    content_id: Optional[str] = None
    content: Optional[str] = None
    metadata_filter: Optional[Dict[str, Any]] = None
class QueryRequest(BaseModel):
    query: str
    n_results: Optional[int] = 5
@app.post("/add_memory")
async def add_memory(content: MemoryContent):
    print("接收到的完整数据:", content.model_dump())
    await GeoFileMemory.add(content, content.filepath)
    return {"status": "success", "message": "内容已成功添加到向量数据库"}
@app.get("/get_memory")
async def get_memory(page: int = 1,page_size: int = 100,metadata_filter: Optional[Dict[str, Any]] = None):
    result = await GeoFileMemory.get_paginated_data(page=page,page_size=page_size,metadata_filter=metadata_filter)
    return result
@app.post("/delete_memory")
async def delete_memory(request: DeleteRequest):
    """
    删除向量数据库中的内容
    支持通过以下三种方式之一删除：
    1. content_id: 通过ID删除
    2. content: 通过内容删除
    3. metadata_filter: 通过元数据条件删除
    """
    if request.content_id:
        await GeoFileMemory.delete_by_id(request.content_id)
        return {"status": "success", "message": f"已删除ID为 {request.content_id} 的内容"}
    elif request.content:
        await GeoFileMemory.delete_by_content(request.content)
        return {"status": "success", "message": "已删除匹配的内容"}
    elif request.metadata_filter:
        await GeoFileMemory.delete_by_metadata(request.metadata_filter)
        return {"status": "success", "message": "已删除匹配元数据条件的内容"}
    else:
        return {"status": "error", "message": "请提供content_id、content或metadata_filter中的至少一个参数"}

@app.get("/clear_memory")
async def clear_memory_get():
    await GeoFileMemory.clear()
    return {"status": "success", "message": "数据库已清空"}

@app.get("/list_modified_data")
async def list_modified_data():
    """
    列出所有被修改的数据
    返回的数据按最后修改时间倒序排列
    """
    result = await GeoFileMemory.list_modified_data()
    return {
        "status": "success",
        "total": result.get("total", 0),
        "modified_data": result.get("modified_data", [])
    }
@app.post("/query_memory")
async def query_memory(request: QueryRequest):
    """
    语义搜索查询接口

    Args:
        query: 查询文本
        n_results: 返回结果数量，默认5条
    """
    result = await GeoFileMemory.query(
        query=request.query,
        n_results=request.n_results
    )
    return {
        "status": "success",
        "results": [
            {
                "content": item.content,
                "metadata": item.metadata
            }
            for item in result.results
        ]
    }
#-----------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------对话-------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------


class ImageData(BaseModel):
    """图像数据模型"""
    image_url: Optional[str] = None
    image_base64: Optional[str] = None
    caption: Optional[str] = None

class ChatRequest(BaseModel):
    query: str
    filelist: Optional[List[str]] = None

@app.post("/chat")
async def chat(
    request: ChatRequest,
):
    return StreamingResponse(event_generator(request.query,request.filelist),media_type="text/event-stream")











#-----------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------服务-------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
        uvicorn.run(app, host="127.0.0.1", port=8000)



import posthog