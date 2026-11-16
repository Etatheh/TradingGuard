
import requests
import time

url = "https://fapi.binance.com/fapi/v1/klines"

params = {
    "symbol": "BTCUSDT",
    "interval": "5m",
    "limit": 3
}

print("正在测试 Binance K线接口...")

try:
    start = time.time()

    response = requests.get(
        url,
        params=params,
        timeout=30
    )

    print("HTTP状态码：", response.status_code)
    print("耗时：", round(time.time() - start, 2), "秒")

    response.raise_for_status()

    data = response.json()

    print("获取K线数量：", len(data))
    print("最新收盘价：", data[-2][4])

except Exception as e:
    print("请求失败：", repr(e))
