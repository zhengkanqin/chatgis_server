import json
from datetime import datetime

from langchain.tools import tool
import httpx
import os

BAIDU_MAPS_API_KEY = os.getenv("BAIDU_MAPS_API_KEY", "mCMglofg1AsrXYSo6SqJ2s0BG22H7ewd")
BASE_URL = "https://api.map.baidu.com"


@tool
def map_geocode(address: str) -> dict:
    """查询地名对应的经纬度坐标，和map_reverse_geocode相对应"""
    resp = httpx.get(f"{BASE_URL}/geocoding/v3/", params={
        "address": address,
        "output": "json",
        "ak": BAIDU_MAPS_API_KEY
    })
    return resp.json()


@tool
def map_reverse_geocode(lat: float, lng: float) -> dict:
    """查询经纬度对应的地址信息，和map_geocode相对应"""
    resp = httpx.get(f"{BASE_URL}/reverse_geocoding/v3/", params={
        "location": f"{lat},{lng}",
        "output": "json",
        "ak": BAIDU_MAPS_API_KEY
    })
    return resp.json()


@tool
def map_search_places(query: str, region: str) -> dict:
    """在指定区域内搜索地点信息"""
    resp = httpx.get(f"{BASE_URL}/place/v2/search", params={
        "query": query,
        "region": region,
        "output": "json",
        "ak": BAIDU_MAPS_API_KEY
    })
    return resp.json()


@tool
def map_place_details(uid: str) -> dict:
    """根据uid获取地点详情"""
    resp = httpx.get(f"{BASE_URL}/place/v2/detail", params={
        "uid": uid,
        "output": "json",
        "scope": 2,
        "ak": BAIDU_MAPS_API_KEY
    })
    return resp.json()


@tool
def map_directions(origin_name:str,origin: str,destination_name:str, destination: str, mode: str = "driving") -> str:
    """路径规划
    origin_name和destination_name填写起始点语义名称
    origin与destination使用纬经度字符串，纬度在前，经度在后（str）:例如"30.14321,37.12314"
    mode 可选（str）：driving, walking, transit, riding
    """
    def format_and_reverse(coord_str: str) -> str:
        lat, lng = map(float, coord_str.split(","))
        return f"{round(lat, 6)},{round(lng, 6)}"  # 转为 lat,lng

    origin_fmt = format_and_reverse(origin)
    destination_fmt = format_and_reverse(destination)
    print(origin_fmt, destination_fmt)

    params = {
        "origin": origin_fmt,
        "destination": destination_fmt,
        "ak": BAIDU_MAPS_API_KEY
    }

    resp = httpx.get(f"{BASE_URL}/directionlite/v1/{mode}", params=params)
    data = resp.json()

    if data.get("status") != 0:
        raise ValueError(f"路线规划失败：{data.get('message')}")

    geojson = convert_to_geojson(data)

    # 保存
    os.makedirs("./GeoFile/Result", exist_ok=True)
    time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"./GeoFile/Result/{origin_name}到{destination_name}-{time_str}.geojson"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    absolute_path = os.path.abspath(filename)

    return f"{origin_name}到{destination_name}的路线规划GeoJSON文件 已保存到：{absolute_path}"


def convert_to_geojson(route_data: dict) -> dict:
    """将百度路径规划结果转为 GeoJSON，保留步骤与总览，并汉化属性"""
    if route_data.get("status") != 0:
        raise ValueError(f"路线规划失败：{route_data.get('message')}")

    result = route_data["result"]
    features = []

    origin = result["origin"]
    features.append({
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [round(origin["lng"], 6), round(origin["lat"], 6)]
        },
        "properties": {"位置": "起点"}
    })

    destination = result["destination"]
    features.append({
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [round(destination["lng"], 6), round(destination["lat"], 6)]
        },
        "properties": {"位置": "终点"}
    })

    route = result["routes"][0]
    all_coords = []

    for step in route["steps"]:
        coords = decode_path(step["path"])
        all_coords.extend(coords)
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": coords
            },
            "properties": {
                "步骤": step.get("instruction", ""),
                "距离": f"{step['distance']} 米",
                "耗时": f"{round(step['duration'] / 60)} 分钟",
                "方向": step.get("direction", ""),
                "转向": step.get("turn", ""),
                "道路类型": step.get("road_type", ""),
                "起点": f"{step['start_location']['lng']},{step['start_location']['lat']}",
                "终点": f"{step['end_location']['lng']},{step['end_location']['lat']}",
            }
        })

    total_distance_km = round(route["distance"] / 1000, 1)
    total_duration_min = round(route["duration"] / 60)

    features.append({
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": all_coords
        },
        "properties": {
            "类型": "路线总览",
            "总距离": f"约 {total_distance_km} 公里",
            "总耗时": f"约 {total_duration_min} 分钟",
            "高速收费": f"{route.get('toll', 0)} 元",
            "路线ID": route.get("route_md5", "")
        }
    })

    return {
        "type": "FeatureCollection",
        "features": features
    }


def decode_path(path_str: str) -> list:
    """解码 path 字符串为坐标列表（保留6位小数）"""
    coords = []
    for pair in path_str.split(";"):
        if not pair.strip():
            continue
        lng, lat = map(float, pair.split(","))
        coords.append([round(lng, 6), round(lat, 6)])
    return coords


@tool
def map_weather(district_id: str) -> dict:
    """获取指定地区的天气信息（使用区县行政编码）"""
    resp = httpx.get(f"{BASE_URL}/weather/v1/", params={
        "district_id": district_id,
        "data_type": "all",
        "ak": BAIDU_MAPS_API_KEY
    })
    return resp.json()




@tool
def map_road_traffic(road_name: str, city: str) -> dict:
    """获取指定城市中某条道路的实时交通信息"""
    resp = httpx.get(f"{BASE_URL}/traffic/v1/road", params={
        "road_name": road_name,
        "city": city,
        "ak": BAIDU_MAPS_API_KEY
    })
    return resp.json()


@tool
def map_poi_extract(text: str) -> dict:
    """从自然语言文本中抽取地点信息（基于NLP）"""
    resp = httpx.post("https://map.baidu.com/nlp/v1/poi", json={
        "text": text,
        "ak": BAIDU_MAPS_API_KEY
    })
    return resp.json()

