import json
import math
import os
from pathlib import Path
from openai import OpenAI


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


CITY_COORDS = {
    "北京": (39.9042, 116.4074),
    "上海": (31.2304, 121.4737),
    "杭州": (30.2741, 120.1551),
}


def get_city_coords(city: str) -> str:
    coords = CITY_COORDS.get(city)
    if not coords:
        return json.dumps({"error": f"未知城市: {city}"}, ensure_ascii=False)
    return json.dumps({"city": city, "lat": coords[0], "lon": coords[1]}, ensure_ascii=False)


def calc_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> str:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    km = 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return json.dumps({"distance_km": round(km, 2)}, ensure_ascii=False)


TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_city_coords",
            "description": "查询中国主要城市的经纬度坐标",
            "parameters": {
                "type": "object",
                "properties": {"city": {"type": "string", "description": "城市名，如北京、上海、杭州"}},
                "required": ["city"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "calc_distance",
            "description": "根据两组经纬度计算球面直线距离（公里）",
            "parameters": {
                "type": "object",
                "properties": {
                    "lat1": {"type": "number"},
                    "lon1": {"type": "number"},
                    "lat2": {"type": "number"},
                    "lon2": {"type": "number"},
                },
                "required": ["lat1", "lon1", "lat2", "lon2"],
            },
        },
    },
]


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    load_env_file(root / ".env")

    base_url = os.environ.get("EVALREEL_PROXY_URL", "http://127.0.0.1:8080/v1")
    client = OpenAI(base_url=base_url, api_key=os.environ["OPENAI_API_KEY"])
    model = os.environ.get("OPENAI_MODEL", "deepseek-v4.1-flash-1017")

    messages = [{"role": "user", "content": "请先查询北京和上海的经纬度，再计算两地直线距离（公里）。"}]

    for turn in range(1, 6):
        print(f"\n--- 第 {turn} 轮请求 ---")
        stream = client.chat.completions.create(
            model=model,
            messages=messages,
            tools=TOOLS,
            stream=True,
            stream_options={"include_usage": True},
        )

        content_parts = []
        tool_map: dict[int, dict] = {}
        finish_reason = None

        for chunk in stream:
            if not chunk.choices:
                continue
            ch = chunk.choices[0]
            if ch.finish_reason:
                finish_reason = ch.finish_reason
            if ch.delta.content:
                content_parts.append(ch.delta.content)
                print(ch.delta.content, end="", flush=True)
            for tc in ch.delta.tool_calls or []:
                item = tool_map.setdefault(tc.index, {"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
                if tc.id:
                    item["id"] = tc.id
                if tc.function:
                    if tc.function.name:
                        item["function"]["name"] += tc.function.name
                    if tc.function.arguments:
                        item["function"]["arguments"] += tc.function.arguments

        assistant_msg: dict = {"role": "assistant", "content": "".join(content_parts) or None}
        if tool_map:
            ordered = [tool_map[idx] for idx in sorted(tool_map)]
            assistant_msg["tool_calls"] = ordered
            messages.append(assistant_msg)

            for tc in ordered:
                name = tc["function"]["name"]
                args = json.loads(tc["function"]["arguments"])
                if name == "get_city_coords":
                    out = get_city_coords(**args)
                elif name == "calc_distance":
                    out = calc_distance(**args)
                else:
                    out = json.dumps({"error": f"unknown tool: {name}"})
                print(f"\n[执行工具] {name}({args}) -> {out}")
                messages.append({"role": "tool", "tool_call_id": tc["id"], "content": out})
            continue

        print(f"\n[完成] finish_reason={finish_reason}")
        break


if __name__ == "__main__":
    main()
