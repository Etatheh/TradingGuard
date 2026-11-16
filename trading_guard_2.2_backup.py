
import asyncio
import aiohttp
import websockets
import json
import os
import time
import hmac
import hashlib
import base64
import urllib.parse
import sys

from pathlib import Path
from collections import deque
from datetime import datetime

# ==========================================
# TRADING GUARD 2.2 - OKX EDITION
# ==========================================

INSTRUMENTS = [
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "SOXL-USDT-SWAP",
]

INTERVALS = ["5m", "15m"]

LOOKBACK = 50
TOUCH_PCT = 0.0015
ALERT_COOLDOWN = 300

MAX_DAILY_LOSSES = 3
MAX_LOSS_STREAK = 3
COOLING_HOURS = 6

REST_URL = "https://www.okx.com/api/v5/market/candles"
WS_URL = "wss://ws.okx.com:8443/ws/v5/business"

DING_WEBHOOK = os.getenv("DING_WEBHOOK", "")
DING_SECRET = os.getenv("DING_SECRET", "")

STATE_FILE = Path("guard_state.json")

history = {}
last_alert = {}
last_closed = {}


# ==========================================
# 1. 交易纪律
# ==========================================

def today():
    return datetime.now().date().isoformat()


def load_state():
    if STATE_FILE.exists():
        try:
            data = json.loads(
                STATE_FILE.read_text(encoding="utf-8")
            )
            if data.get("date") == today():
                return data
        except (OSError, ValueError):
            pass

    return {
        "date": today(),
        "daily_losses": 0,
        "loss_streak": 0,
        "cooldown_until": 0
    }


state = load_state()


def save_state():
    temp = STATE_FILE.with_suffix(".tmp")
    temp.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    temp.replace(STATE_FILE)


def refresh_day():
    if state["date"] != today():
        state.update({
            "date": today(),
            "daily_losses": 0,
            "loss_streak": 0,
            "cooldown_until": 0
        })
        save_state()


def record_trade(result):
    refresh_day()

    if result == "loss":
        state["daily_losses"] += 1
        state["loss_streak"] += 1

    elif result == "win":
        state["loss_streak"] = 0

    else:
        print("请输入 win 或 loss")
        return

    if (
        state["daily_losses"] >= MAX_DAILY_LOSSES
        or state["loss_streak"] >= MAX_LOSS_STREAK
    ):
        state["cooldown_until"] = max(
            state["cooldown_until"],
            time.time() + COOLING_HOURS * 3600
        )

    save_state()
    print(discipline_status())


def trading_locked():
    refresh_day()
    return (
        state["daily_losses"] >= MAX_DAILY_LOSSES
        or state["loss_streak"] >= MAX_LOSS_STREAK
        or time.time() < state["cooldown_until"]
    )


def discipline_status():
    refresh_day()
    remaining = max(
        0,
        state["cooldown_until"] - time.time()
    )

    return (
        f"今日亏损：{state['daily_losses']} 次\n"
        f"连续亏损：{state['loss_streak']} 次\n"
        f"冷静期剩余：{remaining / 3600:.1f} 小时\n"
        f"纪律状态：{'冷静期' if trading_locked() else '正常观察'}"
    )


# ==========================================
# 2. 钉钉推送
# ==========================================

async def notify(session, message):

    if not DING_WEBHOOK:
        print("[钉钉未配置]", message)
        return

    url = DING_WEBHOOK

    if DING_SECRET:
        timestamp = str(int(time.time() * 1000))
        sign_text = f"{timestamp}\n{DING_SECRET}"

        sign = base64.b64encode(
            hmac.new(
                DING_SECRET.encode("utf-8"),
                sign_text.encode("utf-8"),
                hashlib.sha256
            ).digest()
        ).decode()

        separator = "&" if "?" in url else "?"
        url += (
            f"{separator}timestamp={timestamp}"
            f"&sign={urllib.parse.quote_plus(sign)}"
        )

    payload = {
        "msgtype": "text",
        "text": {"content": message}
    }

    try:
        async with session.post(
            url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as response:
            result = await response.json()

            if response.status != 200 or result.get("errcode") != 0:
                print("钉钉推送失败:", result)

    except Exception as e:
        print("钉钉推送异常:", repr(e))


# ==========================================
# 3. 历史K线初始化
# ==========================================

async def load_history(session, inst_id, interval):

    params = {
        "instId": inst_id,
        "bar": interval,
        "limit": "100"
    }

    last_error = None

    for attempt in range(1, 4):
        try:
            async with session.get(
                REST_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as response:

                response.raise_for_status()
                result = await response.json()

            if result.get("code") != "0":
                raise RuntimeError(result.get("msg", result))

            bars = deque(maxlen=LOOKBACK + 5)

            # OKX REST K线按时间倒序返回
            rows = sorted(
                result["data"],
                key=lambda x: int(x[0])
            )

            for row in rows:
                # row[8] == 1 表示K线已经完成
                if len(row) < 9 or row[8] != "1":
                    continue

                bars.append({
                    "time": int(row[0]),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4])
                })

            if len(bars) < 10:
                raise RuntimeError(
                    f"历史K线不足，实际只有 {len(bars)} 根"
                )

            history[(inst_id, interval)] = bars

            print(
                f"[历史数据] {inst_id} {interval}: "
                f"{len(bars)} 根，成功"
            )
            return

        except Exception as e:
            last_error = e
            print(
                f"[重试 {attempt}/3] "
                f"{inst_id} {interval}: {e}"
            )
            if attempt < 3:
                await asyncio.sleep(attempt * 2)

    raise RuntimeError(
        f"{inst_id} {interval} 初始化失败: {last_error}"
    )


# ==========================================
# 4. 支撑与阻力
# ==========================================

def get_levels(inst_id, interval):

    bars = history.get((inst_id, interval))

    if not bars or len(bars) < 10:
        return None, None

    recent = list(bars)[-LOOKBACK:]

    support = min(x["low"] for x in recent)
    resistance = max(x["high"] for x in recent)

    return support, resistance


# ==========================================
# 5. Pinbar识别
# ==========================================

def detect_pinbar(c):

    o = c["open"]
    h = c["high"]
    l = c["low"]
    close = c["close"]

    total = h - l

    if total <= 0:
        return None

    body = abs(close - o)
    body_ref = max(body, total * 0.01)

    upper = h - max(o, close)
    lower = min(o, close) - l

    if (
        lower >= body_ref * 2
        and upper <= body_ref
        and body <= total * 0.35
        and (close - l) / total >= 0.65
    ):
        return "看涨 Pinbar 🟢"

    if (
        upper >= body_ref * 2
        and lower <= body_ref
        and body <= total * 0.35
        and (h - close) / total >= 0.65
    ):
        return "看跌 Pinbar 🔴"

    return None


# ==========================================
# 6. 支撑阻力实时警报
# ==========================================

async def check_levels(
    session, inst_id, interval, price
):

    support, resistance = get_levels(inst_id, interval)

    if support is None:
        return

    checks = [
        ("support", support, "接近支撑位 🟢"),
        ("resistance", resistance, "接近阻力位 🔴")
    ]

    for kind, level, label in checks:

        distance = abs(price - level) / level
        key = (inst_id, interval, kind)
        now = time.monotonic()

        if distance > TOUCH_PCT:
            continue

        if now - last_alert.get(key, 0) < ALERT_COOLDOWN:
            continue

        last_alert[key] = now

        message = (
            f"⚠️ Trading Guard 2.2\n"
            f"标的：{inst_id}\n"
            f"周期：{interval}\n"
            f"状态：{label}\n"
            f"当前价格：{price}\n"
            f"关键价位：{level}\n"
            f"偏离比例：{distance:.3%}\n\n"
            f"{discipline_status()}\n"
            f"请先观察，不要冲动交易。"
        )

        print(message)
        await notify(session, message)


# ==========================================
# 7. K线处理
# ==========================================

async def process_kline(session, arg, row):

    inst_id = arg["instId"]
    interval = arg["channel"].replace("candle", "")

    candle = {
        "time": int(row[0]),
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4])
    }

    # 实时未收盘K线：检查价格
    if row[8] != "1":
        await check_levels(
            session,
            inst_id,
            interval,
            candle["close"]
        )
        return

    key = (inst_id, interval)
    bars = history.get(key)

    if bars is None:
        return

    # 避免重复处理
    if last_closed.get(key) == candle["time"]:
        return

    last_closed[key] = candle["time"]

    support, resistance = get_levels(inst_id, interval)

    pattern = detect_pinbar(candle)

    bars.append(candle)

    print(
        f"[K线收盘] {inst_id} {interval} "
        f"收盘价={candle['close']}"
    )

    if pattern:

        message = (
            f"📊 Trading Guard 形态警报\n"
            f"标的：{inst_id}\n"
            f"周期：{interval}\n"
            f"形态：{pattern}\n"
            f"开盘：{candle['open']}\n"
            f"最高：{candle['high']}\n"
            f"最低：{candle['low']}\n"
            f"收盘：{candle['close']}\n"
            f"此前支撑：{support}\n"
            f"此前阻力：{resistance}\n\n"
            f"{discipline_status()}\n"
            f"仅供观察，不构成交易建议。"
        )

        print(message)
        await notify(session, message)


# ==========================================
# 8. WebSocket订阅
# ==========================================

def build_subscriptions():

    args = []

    for inst_id in INSTRUMENTS:
        for interval in INTERVALS:
            args.append({
                "channel": f"candle{interval}",
                "instId": inst_id
            })

    return args


# ==========================================
# 9. 主监控程序
# ==========================================

async def monitor():

    connector = aiohttp.TCPConnector(
        limit=20,
        ttl_dns_cache=300
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        # 逐个初始化，某个产品失败就明确报错
        for inst_id in INSTRUMENTS:
            for interval in INTERVALS:
                await load_history(
                    session, inst_id, interval
                )

        await notify(
            session,
            "🛡 Trading Guard 2.2 已启动\n"
            "行情源：OKX\n"
            "BTC / ETH / SOL / SOXL\n"
            "周期：5m + 15m\n"
            "支撑阻力：开启\n"
            "Pinbar识别：开启\n"
            "交易纪律：开启\n\n"
            + discipline_status()
        )

        while True:

            try:
                print("\n正在连接 OKX WebSocket...")

                async with websockets.connect(
                    WS_URL,
                    ping_interval=None,
                    close_timeout=10,
                    open_timeout=20
                ) as ws:

                    await ws.send(json.dumps({
                        "op": "subscribe",
                        "args": build_subscriptions()
                    }))

                    print("OKX WebSocket 已连接，等待订阅...")

                    async for raw in ws:

                        if raw == "ping":
                            await ws.send("pong")
                            continue

                        packet = json.loads(raw)

                        if packet.get("event") == "error":
                            print("OKX订阅错误:", packet)
                            continue

                        if "arg" not in packet:
                            continue

                        arg = packet["arg"]

                        for row in packet.get("data", []):
                            await process_kline(
                                session, arg, row
                            )

            except Exception as e:

                print("WebSocket连接异常:", repr(e))
                print("5秒后重新连接...")

                await asyncio.sleep(5)

                # 重连后重新同步历史K线
                for inst_id in INSTRUMENTS:
                    for interval in INTERVALS:
                        try:
                            await load_history(
                                session, inst_id, interval
                            )
                        except Exception as err:
                            print("历史数据恢复失败:", err)


# ==========================================
# 10. 命令行纪律管理
# ==========================================

def main():

    if len(sys.argv) > 1:

        command = sys.argv[1].lower()

        if command == "record-loss":
            record_trade("loss")

        elif command == "record-win":
            record_trade("win")

        elif command == "status":
            print(discipline_status())

        elif command == "reset-streak":
            state["loss_streak"] = 0
            save_state()
            print("连续亏损计数已重置")

        else:
            print(
                "命令：record-loss / record-win / "
                "status / reset-streak"
            )

        return

    asyncio.run(monitor())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nTrading Guard 已停止。")
