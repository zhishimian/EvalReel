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


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    load_env_file(root / ".env")

    base_url = os.environ.get("EVALREEL_PROXY_URL", "http://127.0.0.1:8080/v1")
    api_key = os.environ["OPENAI_API_KEY"]
    model = os.environ.get("OPENAI_MODEL", "deepseek-v4.1-flash-1017")

    client = OpenAI(base_url=base_url, api_key=api_key)

    print("=== 1. 查询可用模型列表 (/v1/models) ===")
    try:
        models = client.models.list()
        ids = [m.id for m in models.data]
        print(f"返回模型数量: {len(ids)}")
        for mid in ids:
            print(f"  - {mid}")
    except Exception as exc:
        print(f"查询 /v1/models 失败或未开放: {exc}")

    print(f"\n=== 2. 流式对话冒烟测试 (model={model}, via={base_url}) ===")
    stream = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": "请用两句话说明什么是空间索引，并保持极简回答。"}],
        stream=True,
        stream_options={"include_usage": True},
        max_tokens=200,
    )

    reasoning_printed = False
    content_printed = False
    usage_info = None

    for chunk in stream:
        if getattr(chunk, "usage", None):
            usage_info = chunk.usage
        if not chunk.choices:
            continue
        delta = chunk.choices[0].delta
        rc = getattr(delta, "reasoning_content", None)
        if not rc and getattr(delta, "model_extra", None):
            rc = delta.model_extra.get("reasoning_content")
        if rc:
            if not reasoning_printed:
                print("\n[思考过程] ", end="", flush=True)
                reasoning_printed = True
            print(rc, end="", flush=True)
        if delta.content:
            if not content_printed:
                print("\n[正式回答] ", end="", flush=True)
                content_printed = True
            print(delta.content, end="", flush=True)

    print("\n\n=== 3. 用量统计 ===")
    print(usage_info)


if __name__ == "__main__":
    main()
