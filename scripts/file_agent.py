import json
import os
import sys
from pathlib import Path
from openai import OpenAI


def load_env_if_missing() -> None:
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if not env_file.exists():
        return
    for raw in env_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def safe_path(rel_path: str) -> Path:
    root = Path.cwd().resolve()
    target = (root / rel_path).resolve()
    target.relative_to(root)
    return target


def read_file(path: str) -> str:
    try:
        p = safe_path(path)
        if not p.is_file():
            return json.dumps({"error": f"file not found: {path}"}, ensure_ascii=False)
        return json.dumps({"path": path, "content": p.read_text(encoding="utf-8")}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"error": str(exc)}, ensure_ascii=False)


def write_file(path: str, content: str) -> str:
    try:
        p = safe_path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        return json.dumps({"status": "ok", "path": path, "bytes": len(content.encode("utf-8"))}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"error": str(exc)}, ensure_ascii=False)


TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "读取当前工作目录下的文本文件",
            "parameters": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "在当前工作目录下写入或新建文本文件",
            "parameters": {
                "type": "object",
                "properties": {"path": {"type": "string"}, "content": {"type": "string"}},
                "required": ["path", "content"],
            },
        },
    },
]


def main() -> None:
    load_env_if_missing()
    base_url = os.environ.get("OPENAI_BASE_URL") or os.environ.get("EVALREEL_PROXY_URL", "http://127.0.0.1:8080/v1")
    client = OpenAI(base_url=base_url, api_key=os.environ["OPENAI_API_KEY"])
    model = os.environ.get("OPENAI_MODEL", "qwen3.8-flash")
    task = sys.argv[1] if len(sys.argv) > 1 else "请在当前目录创建一个 math_utils.py，实现 add(a, b) 函数，然后用 read_file 读一遍确认内容正确。"
    messages = [{"role": "user", "content": task}]

    for _ in range(6):
        resp = client.chat.completions.create(model=model, messages=messages, tools=TOOLS, stream=False)
        msg = resp.choices[0].message
        tool_calls = msg.tool_calls or []
        assistant_entry = {"role": "assistant", "content": msg.content}
        if tool_calls:
            assistant_entry["tool_calls"] = [tc.model_dump() for tc in tool_calls]
        messages.append(assistant_entry)
        if not tool_calls:
            print(msg.content or "")
            return
        for tc in tool_calls:
            args = json.loads(tc.function.arguments)
            out = read_file(**args) if tc.function.name == "read_file" else write_file(**args) if tc.function.name == "write_file" else json.dumps({"error": "unknown tool"})
            print(f"[tool] {tc.function.name} -> {out}")
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": out})
    raise RuntimeError("exceeded max turns")


if __name__ == "__main__":
    main()
