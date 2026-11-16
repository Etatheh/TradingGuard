
import os
import hmac
import hashlib
import base64
import time
import urllib.parse
import aiohttp
import asyncio

async def main():
    webhook = os.getenv("DING_WEBHOOK", "")
    secret = os.getenv("DING_SECRET", "")

    if not webhook or not secret:
        print("错误：没有读取到钉钉配置")
        return

    timestamp = str(round(time.time() * 1000))
    sign_string = f"{timestamp}\n{secret}"

    sign = base64.b64encode(
        hmac.new(
            secret.encode("utf-8"),
            sign_string.encode("utf-8"),
            hashlib.sha256
        ).digest()
    ).decode("utf-8")

    separator = "&" if "?" in webhook else "?"
    url = (
        f"{webhook}{separator}"
        f"timestamp={timestamp}&"
        f"sign={urllib.parse.quote_plus(sign)}"
    )

    payload = {
        "msgtype": "text",
        "text": {
            "content": " Trading Guard 2.0\n"
                       "钉钉连接测试成功！\n"
                       "BTC / ETH / SOL 监控系统准备就绪。"
        }
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            result = await response.json()
            print("钉钉返回结果：", result)

            if result.get("errcode") == 0:
                print("推送成功！")
            else:
                print("推送失败，请检查机器人配置。")

asyncio.run(main())
