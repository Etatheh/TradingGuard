
import requests

symbols = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "SOXLUSDT"
]

url = "https://fapi.binance.com/fapi/v1/exchangeInfo"

try:
    response = requests.get(url, timeout=15)
    response.raise_for_status()

    data = response.json()

    available = {
        item["symbol"]
        for item in data["symbols"]
    }

    print("\n===== Binance Futures 检查 =====")

    for symbol in symbols:
        result = "OK" if symbol in available else "NOT FOUND"
        print(symbol, result)

except Exception as e:
    print("请求失败：", e)
