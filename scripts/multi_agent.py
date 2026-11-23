#!/usr/bin/env python3
"""极简多 Agent 嵌套调用演示脚本（仅依赖 Python 标准库）：
展示如何在父子 Agent（planner -> coder -> reviewer）之间透传 W3C traceparent 请求头，
让 EvalReel Proxy 自动把多跳请求串联成白盒父子调用树。
"""

import json
import os
import secrets
import sys
import urllib.request


def parse_traceparent(raw: str) -> tuple[str, str]:
    parts = raw.strip().lower().split("-")
    if len(parts) == 4 and len(parts[1]) == 32 and len(parts[2]) == 16:
        return parts[1], parts[2]
    return secrets.token_hex(16), secrets.token_hex(8)


def call_llm(
    base_url: str,
    api_key: str,
    model: str,
    trace_id: str,
    parent_span_id: str,
    agent_name: str,
    prompt: str,
) -> tuple[str, str]:
    url = base_url.rstrip("/")
    if not url.endswith("/chat/completions"):
        if url.endswith("/v1"):
            url = url + "/chat/completions"
        else:
            url = url + "/v1/chat/completions"

    payload = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
    }).encode("utf-8")

    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    req.add_header("traceparent", f"00-{trace_id}-{parent_span_id}-01")
    req.add_header("X-EvalReel-Agent-Name", agent_name)
    if os.environ.get("EVALREEL_RUN_ID"):
        req.add_header("X-EvalReel-Run-ID", os.environ["EVALREEL_RUN_ID"])
    if os.environ.get("EVALREEL_TASK_ID"):
        req.add_header("X-EvalReel-Task-ID", os.environ["EVALREEL_TASK_ID"])

    with urllib.request.urlopen(req, timeout=30) as resp:
        span_id = resp.headers.get("X-EvalReel-Span-ID") or secrets.token_hex(8)
        data = json.loads(resp.read().decode("utf-8"))
        content = ""
        choices = data.get("choices") or []
        if choices:
            content = choices[0].get("message", {}).get("content", "")
        return span_id, content


def main() -> int:
    task = " ".join(sys.argv[1:]).strip() or "编写并检查一个计算两数之和的函数"
    base_url = os.environ.get("OPENAI_BASE_URL", "http://127.0.0.1:8080/v1")
    api_key = os.environ.get("OPENAI_API_KEY", "sk-mock")
    model = os.environ.get("OPENAI_MODEL", "mock-reasoning-v1")

    trace_id, root_span = parse_traceparent(os.environ.get("TRACEPARENT", ""))

    # 1. 顶层规划器（planner）先拆解任务，获取代理返回给该请求的实际 span_id。
    planner_span, plan_text = call_llm(
        base_url, api_key, model, trace_id, root_span, "planner", f"请拆解任务: {task}"
    )

    # 2. 编码器（coder）以 planner 的 span_id 作为父节点发起实现请求。
    coder_span, code_text = call_llm(
        base_url, api_key, model, trace_id, planner_span, "coder", f"按计划实现代码: {plan_text[:120]}"
    )

    # 3. 审查器（reviewer）以 coder 的 span_id 作为父节点完成检查，形成 3 层嵌套链。
    _, review_text = call_llm(
        base_url, api_key, model, trace_id, coder_span, "reviewer", f"检查实现是否存在边界问题: {code_text[:120]}"
    )

    print(json.dumps({
        "trace_id": trace_id,
        "plan": plan_text[:80],
        "code": code_text[:80],
        "review": review_text[:80],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
