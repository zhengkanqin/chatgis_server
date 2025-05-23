# backend/main.py
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from chat_handler import handle_chat
from connection_manager import manager
from fastapi.responses import StreamingResponse
import uvicorn
from agent_config import agent
app = FastAPI()

# CORS 中间件设置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 开发阶段允许所有来源
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket 路由
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            print(f"Received message: {data}")
            await websocket.send_text(f"Response: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("WebSocket disconnected")

# 普通 HTTP 路由
@app.get("/chat")
async def chat(q: str):
    response = await handle_chat(q)  # 调用 handle_chat 获取最终响应
    return {"response": response}

@app.get("/chat_stream")
async def chat_stream(q: str):
    async def generate():
        async for chunk in agent.run_stream(task=q):
            if hasattr(chunk, 'content'):
                yield f"data: {chunk.content}\n\n"
            elif hasattr(chunk, 'messages'):
                for message in chunk.messages:
                    if hasattr(message, 'content'):
                        yield f"data: {message.content}\n\n"
    return StreamingResponse(generate(),media_type="text/event-stream")

# 启动服务
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)


