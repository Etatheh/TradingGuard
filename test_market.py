
import asyncio
import json
import websockets
from datetime import datetime

SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]

streams = "/".join(
    f"{symbol}@trade" for symbol in SYMBOLS
)

URL = (
    "wss://stream.binance.com:9443/stream"
    f"?streams={streams}"
)

async def main():
    print("正在连接 Binance...")
    print("监控 BTC / ETH / SOL")
    print("按 Ctrl+C 停止\n")

    async with websockets.connect(
        URL,
        ping_interval=20,
        ping_timeout=20
    ) as ws:

        print("连接成功！等待实时行情...\n")

        async for raw in ws:
            packet = json.loads(raw)
            data = packet["data"]

            symbol = data["s"]
            price = float(data["p"])

            now = datetime.now().strftime("%H:%M:%S")

            print(
                f"[{now}] "
                f"{symbol:<10} "
                f"${price:,.4f}"
            )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n行情监控已停止")
