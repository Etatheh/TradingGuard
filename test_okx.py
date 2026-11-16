
import requests

symbols = [
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "SOXL-USDT-SWAP"
]

url = "https://www.okx.com/api/v5/market/candles"

for symbol in symbols:
    for interval in ["5m", "15m"]:
        try:
            response = requests.get(
                url,
                params={
                    "instId": symbol,
                    "bar": interval,
                    "limit": "3"
                },
                timeout=15
            )

            response.raise_for_status()
            result = response.json()

            if result["code"] != "0":
                print("FAIL", symbol, interval, result)
                continue

            data = result["data"]

            print(
                "OK |", symbol,
                "|", interval,
                "| 获取", len(data), "根K线"
            )

        except Exception as e:
            print("FAIL |", symbol, interval, repr(e))
