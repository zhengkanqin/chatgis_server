from langchain.tools import tool
import httpx
import os

BAIDU_MAPS_API_KEY = os.getenv("BAIDU_MAPS_API_KEY", "mCMglofg1AsrXYSo6SqJ2s0BG22H7ewd")
BASE_URL = "https://api.map.baidu.com"


@tool
def map_geocode(address: str) -> dict:
    """根据地址返回经纬度坐标"""
    resp = httpx.get(f"{BASE_URL}/geocoding/v3/", params={
        "address": address,
        "output": "json",
        "ak": BAIDU_MAPS_API_KEY
    })
    return resp.json()


@tool
def map_reverse_geocode(lat: float, lng: float) -> dict:
    """根据经纬度返回详细地址信息"""
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
def map_directions(origin: str, destination: str, mode: str = "driving") -> dict:
    """路径规划，mode 可选：driving, walking, transit, riding"""
    resp = httpx.get(f"{BASE_URL}/directionlite/v1/{mode}", params={
        "origin": origin,
        "destination": destination,
        "ak": BAIDU_MAPS_API_KEY
    })
    return resp.json()


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
def map_ip_location(ip: str = "") -> dict:
    """根据 IP 地址获取地理位置（默认获取当前 IP）"""
    resp = httpx.get(f"{BASE_URL}/location/ip", params={
        "ip": ip,
        "coor": "bd09ll",
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
