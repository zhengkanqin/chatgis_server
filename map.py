import json
from langchain_core.tools import tool
from connection_manager import manager

@tool()
async def draw_boundary(name:str):
    """
    在用户可见的地图上绘制城市或者区域的边界

    参数:
    - name: 城市或者地区的名字
    """
    CommandEvent = {"type": "map",
                    "operation": "draw-boundary",
                    "data": name}
    json_str = json.dumps(CommandEvent)  # 转成 JSON 字符串
    await manager.send_message(json_str)
    return "绘制成功"

# 绘制点标记
@tool()
async def draw_point(point: str, name: str):
    """
    在用户可见的地图上绘制一个点

    参数:
    - point: [x,y] 点的坐标
    - name: str 点的名字
    """
    CommandEvent = {
        "type": "map",
        "operation": "draw-point",
        "data": {
            "point": point,
            "name": name
        }
    }
    json_str = json.dumps(CommandEvent)  # 转成 JSON 字符串
    await manager.send_message(json_str)
    return "绘制成功"
@tool()
async def draw_line(points: str, name: str):
    """
    在用户可见的地图上绘制一条线

    参数:
    - points: [[x,y],[x,y],...] 点的坐标序列
    - name: str 线的名字
    """
    CommandEvent = {
        "type": "map",
        "operation": "draw-line",
        "data": {
            "points": points,
            "name": name
        }
    }
    json_str = json.dumps(CommandEvent)  # 转成 JSON 字符串
    await manager.send_message(json_str)
    return "绘制成功"

# 绘制多边形
@tool()
async def draw_polygon(points: str, name: str):
    """
    在用户可见的地图上绘制一个面

    参数:
    - points: [[x,y],[x,y],...] 点的坐标序列
    - name: str 面的名字
    """
    CommandEvent = {
        "type": "map",
        "operation": "draw-polygon",
        "data": {
            "points": points,
            "name": name
        }
    }
    json_str = json.dumps(CommandEvent)  # 转成 JSON 字符串
    await manager.send_message(json_str)
    return "绘制成功"

# 绘制圆
@tool()
async def draw_circle(point: str, radius: str, name: str):
    """
    在用户可见的地图上绘制一个圆

    参数:
    - point: [x,y] 圆点坐标
    - radius: str 圆的半径(只需数字，单位是米)
    - name: str 圆的名字
    """
    CommandEvent = {
        "type": "map",
        "operation": "draw-circle",
        "data": {
            "point": point,
            "radius": radius,
            "name": name
        }
    }
    json_str = json.dumps(CommandEvent)  # 转成 JSON 字符串
    await manager.send_message(json_str)
    return "绘制成功"

# 绘制矩形
@tool()
async def draw_rectangle(points: str, name: str):
    """
    在用户可见的地图上绘制一个矩形

    参数:
    - points: [[x,y],[x,y],[x,y],[x,y]] 四个坐标点确认一个矩形
    - name: str 矩形的名字
    """
    CommandEvent = {
        "type": "map",
        "operation": "draw-rectangle",
        "data": {
            "points": points,
            "name": name
        }
    }
    json_str = json.dumps(CommandEvent)  # 转成 JSON 字符串
    await manager.send_message(json_str)
    return "绘制成功"


# 绘制文本
@tool()
async def draw_label(point: str, name: str):
    """
    在用户可见的地图上绘制一个标签

    参数:
    - points: [x,y] 标签的坐标点
    - name: str 标签的内容
    """
    CommandEvent = {
        "type": "map",
        "operation": "draw-label",
        "data": {
            "point": point,
            "name": name
        }
    }
    json_str = json.dumps(CommandEvent)  # 转成 JSON 字符串
    await manager.send_message(json_str)
    return "绘制成功"

# 绘制图片
@tool()
async def draw_image(url: str, bounds: str, name: str, options: str):
    """
    在用户可见的地图上绘制一个地图瓦片

    参数:
    - url: str 地图瓦片的路径或链接
    - bounds: [[x,y],[x,y]] 东北和西南角坐标确定的瓦片边界
    - name: str 地图瓦片的名字
    """
    CommandEvent = {
        "type": "map",
        "operation": "draw-image",
        "data": {
            "url": url,
            "bounds": bounds,
            "name": name,
            "options": options
        }
    }
    json_str = json.dumps(CommandEvent)  # 转成 JSON 字符串
    await manager.send_message(json_str)
    return "绘制成功"

# 绘制geojson
@tool()
async def draw_geojson(geojson: str, name: str, style: str, properties:str):
    """
    在用户可见的地图上绘制一个GeoJSON图层

    参数:
    - geojson: str GeoJSON格式数据、GeoJSON链接、GeoJSON路径均可
    - name: str GeoJSON图层的名字
    - style: str 默认不写，可以输入部分json，可选的属性字段，默认为{strokeColor: "#0000ff",strokeWeight: 2,strokeOpacity: 0.8,fillColor: "#ffcccc",fillOpacity: 0.4,enableEditing: false,enableClicking: true}，
    - properties: str 可以指定用于数值排序的属性，根据需要选择是否填写
    """

    CommandEvent = {
        "type": "map",
        "operation": "draw-geojson",
        "data": {
            "geojson": geojson,
            "name": name,
            "style": style,
            "properties": properties
        }
    }
    print(CommandEvent)
    json_str = json.dumps(CommandEvent)  # 转成 JSON 字符串
    await manager.send_message(json_str)
    return "绘制成功"

