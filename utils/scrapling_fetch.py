#!/usr/bin/env python3
"""
Scrapling 封装的 HTTP 请求工具
用于替代 bb-browser fetch，通过 Scrapling 的 AsyncFetcher 直连 push2 API

使用方式：
    python3 scrapling_fetch.py <url> [--json]
    
输出：
    --json 可选，强制按JSON格式输出
    成功: {"success":true, "data": <响应体>}
    失败: {"success":false, "error": "..."}
"""
import sys
import json
import asyncio
import os

# 添加 .scrapling_venv 到路径
_venv_base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".scrapling_venv")
_site_packages = None
for path in [
    os.path.join(_venv_base, "lib", d, "site-packages")
    for d in (os.listdir(os.path.join(_venv_base, "lib")) if os.path.isdir(os.path.join(_venv_base, "lib")) else [])
]:
    if os.path.isdir(path):
        _site_packages = path
        break

if _site_packages:
    sys.path.insert(0, _site_packages)


async def fetch_url(url: str) -> dict:
    """异步获取 URL 并返回 JSON"""
    try:
        from scrapling.fetchers import AsyncFetcher

        response = await AsyncFetcher.get(url)
        if response.status != 200:
            return {"success": False, "error": f"HTTP {response.status}", "status": response.status}

        # 先尝试解析 JSON
        try:
            data = response.json()
            return {"success": True, "data": data}
        except Exception:
            # 返回原始文本
            return {"success": True, "data": response.body.decode("utf-8", errors="replace") if response.body else ""}

    except ImportError as e:
        return {"success": False, "error": f"Scrapling 未安装: {e}"}
    except Exception as e:
        return {"success": False, "error": f"{type(e).__name__}: {str(e)[:200]}"}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"success": False, "error": "用法: python3 scrapling_fetch.py <url>"}))
        sys.exit(1)

    url = sys.argv[1]
    result = asyncio.run(fetch_url(url))
    print(json.dumps(result, ensure_ascii=False))
