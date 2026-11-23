#!/usr/bin/env python3
"""极简标准库 MCP 工具演示服务端（支持 --transport stdio|http）。
用于本地测试 EvalReel MCP 透明代理的录制、只读 Mock 回放与工具侧故障注入。
"""

import argparse
import json
import math
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


TOOLS = [
    {
        "name": "get_weather",
        "description": "查询指定城市天气",
        "inputSchema": {
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
    },
    {
        "name": "calc_distance",
        "description": "计算平面两点欧氏距离",
        "inputSchema": {
            "type": "object",
            "properties": {
                "x1": {"type": "number"},
                "y1": {"type": "number"},
                "x2": {"type": "number"},
                "y2": {"type": "number"},
            },
            "required": ["x1", "y1", "x2", "y2"],
        },
    },
]


def handle_rpc(msg: dict) -> dict | None:
    msg_id = msg.get("id")
    method = msg.get("method", "")
    params = msg.get("params") or {}

    if msg_id is None:
        return None

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "serverInfo": {"name": "evalreel-mock-mcp", "version": "1.0.0"},
                "capabilities": {"tools": {}},
            },
        }
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": msg_id, "result": {"tools": TOOLS}}
    if method == "tools/call":
        name = params.get("name", "")
        args = params.get("arguments") or {}
        if name == "get_weather":
            city = args.get("city", "Beijing")
            text = f"{city}: 晴朗 23C"
        elif name == "calc_distance":
            d = math.hypot(float(args.get("x2", 0)) - float(args.get("x1", 0)),
                           float(args.get("y2", 0)) - float(args.get("y1", 0)))
            text = f"{d:.2f}"
        else:
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "error": {"code": -32601, "message": f"unknown tool: {name}"},
            }
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"content": [{"type": "text", "text": text}], "isError": False},
        }
    return {
        "jsonrpc": "2.0",
        "id": msg_id,
        "error": {"code": -32601, "message": f"method not found: {method}"},
    }


def serve_stdio() -> None:
    for line in sys.stdin:
        raw = line.strip()
        if not raw:
            continue
        resp = handle_rpc(json.loads(raw))
        if resp is not None:
            sys.stdout.write(json.dumps(resp, ensure_ascii=False) + "\n")
            sys.stdout.flush()


class MCPHTTPHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        resp = handle_rpc(json.loads(body.decode("utf-8")))
        out = json.dumps(resp or {}, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, format: str, *args) -> None:
        return


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    parser.add_argument("--port", type=int, default=9191)
    args = parser.parse_args()

    if args.transport == "stdio":
        serve_stdio()
    else:
        HTTPServer(("127.0.0.1", args.port), MCPHTTPHandler).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
