#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trading Guard 2.3 - OKX Edition
===============================

监控 OKX 永续合约的 5m / 15m K线，做三件事：
  1) 支撑 / 阻力识别 + 接近提醒
  2) Pinbar 形态识别
  3) 交易纪律统计（每日亏损次数 / 连续亏损 / 6 小时冷静期）

安全边界（硬性约束，写在代码里而不是写在文档里）：
  * 只读行情：REST 只允许访问 /api/v5/market/* 与 /api/v5/public/*，见 okx_get_json()
  * 不需要 API Key，代码里没有任何签名 / 下单 / 撤单逻辑
  * 不会自动交易，只推送提醒

相对 2.2 的修复点（保持原有分层结构，未推倒重写）：
  * aiohttp 与 requests 的网络差异：显式代理（环境变量 / Windows 系统代理）、
    统一 Header、连接与读取超时、错误分类、指数退避重试、每次失败打印 "第几次/为什么"
  * 合约可用性校验：启动先查 /api/v5/public/instruments，SOXL 不存在时明确报错并跳过，
    而不是让整个程序崩在第一个交易对上
  * 单个交易对失败不再拖垮全局：逐个初始化 + 后台补数据
  * K线完成标记 confirm 兼容字符串/数字；OHLC 自洽性校验；按时间排序去重
  * 历史 → 实时衔接：last_closed 去重、缺口检测、重连后先补历史再订阅
  * WebSocket：订阅回执 / 错误事件处理、服务端文本 ping-pong、90s 无消息看门狗、
    指数退避重连、断线自动重订阅
  * 钉钉：加签 URL 独立成函数、失败重试、errcode 判断、超长截断、异常不吞
  * 日志：logging + 滚动文件 + UTF-8 控制台（修复 Windows GBK 下 emoji 触发
    UnicodeEncodeError 导致监控中断的真实问题）
  * 交易纪律：跨零点保留仍在生效的冷静期；状态文件原子写入

命令行：
  py trading_guard.py                 # 启动监控
  py trading_guard.py status
  py trading_guard.py record-loss
  py trading_guard.py record-win
  py trading_guard.py reset-streak
  py trading_guard.py verify          # 校验 4 个合约 + 8 组K线是否真的可用
  py trading_guard.py diag            # 网络 / 代理 / 接口全链路诊断
  py trading_guard.py selftest        # 离线自测（不联网）
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import hmac
import inspect
import json
import logging
import os
import random
import socket
import ssl
import sys
import time
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterable, Sequence

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

__version__ = "2.3"

# ==========================================
# 0. 默认配置
# ==========================================

INSTRUMENTS: tuple[str, ...] = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "SOXL-USDT-SWAP",
)

INTERVALS: tuple[str, ...] = ("5m", "15m")

LOOKBACK = 50            # 支撑阻力回看多少根已收盘K线
TOUCH_PCT = 0.0015       # 距离关键位 0.15% 内提醒
ALERT_COOLDOWN = 300     # 同一交易对/周期/方向 5 分钟内不重复提醒

MAX_DAILY_LOSSES = 3
MAX_LOSS_STREAK = 3
COOLING_HOURS = 6

MIN_BARS = 10            # 少于这个根数视为历史数据不可用
CANDLE_LIMIT = 100       # OKX /market/candles 单次上限
MAX_BARS = 400           # 内存里最多保留多少根

OKX_REST_BASE = os.getenv("OKX_REST_BASE", "https://www.okx.com")
OKX_WS_URL = os.getenv("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/business")

DING_WEBHOOK = os.getenv("DING_WEBHOOK", "")
DING_SECRET = os.getenv("DING_SECRET", "")

PROJECT_DIR = Path(__file__).resolve().parent

# 只读白名单：任何写操作/交易接口都会被 okx_get_json 拒绝
READ_ONLY_PREFIXES = ("/api/v5/market/", "/api/v5/public/")

# OKX 业务错误码中值得重试的（50011 限流，50001 服务暂不可用，50004 接口超时）
RETRYABLE_CODES = {"50001", "50004", "50011", "50013"}


@dataclass
class Config:
    symbols: tuple[str, ...] = INSTRUMENTS
    intervals: tuple[str, ...] = INTERVALS
    lookback: int = LOOKBACK
    touch_pct: float = TOUCH_PCT
    alert_cooldown: float = ALERT_COOLDOWN
    max_daily_losses: int = MAX_DAILY_LOSSES
    max_loss_streak: int = MAX_LOSS_STREAK
    cooling_hours: float = COOLING_HOURS
    rest_base: str = OKX_REST_BASE
    ws_url: str = OKX_WS_URL
    candle_limit: int = CANDLE_LIMIT
    max_bars: int = MAX_BARS
    http_timeout: float = 20.0
    idle_timeout: float = 90.0
    proxy: str | None = None
    proxy_source: str = "直连"
    insecure: bool = False
    duration: float = 0.0
    notify_stop: bool = False
    state_file: Path = field(default_factory=lambda: PROJECT_DIR / "guard_state.json")
    log_file: Path = field(default_factory=lambda: PROJECT_DIR / "trading_guard.log")
    ding_webhook: str = DING_WEBHOOK
    ding_secret: str = DING_SECRET
    ding_enabled: bool = True
    log_level: str = "INFO"


CONFIG = Config()

DEFAULT_HEADERS = {
    "User-Agent": os.getenv("TG_USER_AGENT", f"TradingGuard/{__version__}"),
    "Accept": "application/json",
}

log = logging.getLogger("trading_guard")

# ==========================================
# 1. 日志与编码
# ==========================================


def _force_utf8(stream: Any) -> None:
    """把 stdout/stderr 切到 UTF-8。

    Windows 下输出被重定向时 sys.stdout.encoding 常常是 gbk，
    打印 🟢/🔴/📊 会抛 UnicodeEncodeError，直接把监控循环打断。
    """
    if stream is None:
        return
    encoding = (getattr(stream, "encoding", "") or "").lower().replace("-", "")
    if encoding in ("utf8", "utf8mb4"):
        return
    reconfigure = getattr(stream, "reconfigure", None)
    if reconfigure is None:
        return
    try:
        reconfigure(encoding="utf-8", errors="replace")
    except (OSError, ValueError):
        # 某些被包装过的流不支持 reconfigure，忽略即可，
        # 后面 _safe_text() 还会兜底一次。
        pass


def _safe_text(text: str) -> str:
    """控制台不支持某些字符时用 ? 代替，绝不抛异常。"""
    stream = sys.stdout
    encoding = getattr(stream, "encoding", None) or "utf-8"
    try:
        text.encode(encoding)
        return text
    except (UnicodeEncodeError, LookupError):
        return text.encode(encoding, "replace").decode(encoding, "replace")


def setup_logging(level: str = "INFO", log_file: Path | None = None) -> None:
    """控制台 + 滚动文件双输出，重复调用不会重复挂 handler。"""
    _force_utf8(sys.stdout)
    _force_utf8(sys.stderr)
    if sys.stdout is None:
        sys.stdout = open(os.devnull, "w", encoding="utf-8")
    if sys.stderr is None:
        sys.stderr = sys.stdout

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)

    root.setLevel(getattr(logging, str(level).upper(), logging.INFO))
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)

    if log_file is not None:
        try:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            file_handler = RotatingFileHandler(
                log_file, maxBytes=2_000_000, backupCount=3, encoding="utf-8"
            )
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)
        except OSError as exc:
            log.warning("日志文件不可写，仅输出到控制台：%s", exc)


def safe_print(message: str = "") -> None:
    try:
        print(_safe_text(message), flush=True)
    except Exception:  # noqa: BLE001 - 输出永远不能影响监控
        pass


def fmt_ts(ms: int) -> str:
    try:
        return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, OverflowError, ValueError):
        return str(ms)


# ==========================================
# 2. 交易纪律
# ==========================================


def today() -> str:
    return datetime.now().date().isoformat()


def _fresh_state() -> dict:
    return {
        "date": today(),
        "daily_losses": 0,
        "loss_streak": 0,
        "cooldown_until": 0.0,
    }


def load_state() -> dict:
    """读取纪律状态；文件损坏/缺字段都不会让程序起不来。"""
    fresh = _fresh_state()
    path = CONFIG.state_file

    if not path.exists():
        return fresh

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        log.warning("状态文件读取失败，本次使用默认值：%s", exc)
        return fresh

    if not isinstance(raw, dict):
        log.warning("状态文件格式异常，本次使用默认值")
        return fresh

    def _as_int(key: str, default: int = 0) -> int:
        try:
            return max(0, int(raw.get(key, default) or 0))
        except (TypeError, ValueError):
            return default

    def _as_float(key: str, default: float = 0.0) -> float:
        try:
            return max(0.0, float(raw.get(key, default) or 0.0))
        except (TypeError, ValueError):
            return default

    out = _fresh_state()
    out["daily_losses"] = _as_int("daily_losses")
    out["loss_streak"] = _as_int("loss_streak")
    out["cooldown_until"] = _as_float("cooldown_until")

    if raw.get("date") != fresh["date"]:
        # 跨日：当日计数归零，但仍在生效的冷静期必须保留
        out["daily_losses"] = 0
        out["loss_streak"] = 0
        if out["cooldown_until"] <= time.time():
            out["cooldown_until"] = 0.0

    return out


state: dict = load_state()


def save_state() -> None:
    """原子写入，避免断电/崩溃留下半个 JSON。"""
    path = CONFIG.state_file
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp.replace(path)
    except OSError as exc:
        log.error("状态文件保存失败（纪律计数可能丢失）：%s", exc)


def refresh_day() -> None:
    if state.get("date") != today():
        cooldown = float(state.get("cooldown_until", 0.0) or 0.0)
        state.update(
            {
                "date": today(),
                "daily_losses": 0,
                "loss_streak": 0,
                "cooldown_until": cooldown if cooldown > time.time() else 0.0,
            }
        )
        save_state()


def trading_locked() -> bool:
    refresh_day()
    if int(state.get("daily_losses", 0)) >= CONFIG.max_daily_losses:
        return True
    if int(state.get("loss_streak", 0)) >= CONFIG.max_loss_streak:
        return True
    return time.time() < float(state.get("cooldown_until", 0.0) or 0.0)


def discipline_status() -> str:
    refresh_day()
    remaining = max(0.0, float(state.get("cooldown_until", 0.0) or 0.0) - time.time())
    locked = trading_locked()
    return "\n".join(
        [
            f"今日亏损：{int(state.get('daily_losses', 0))} / {CONFIG.max_daily_losses} 次",
            f"连续亏损：{int(state.get('loss_streak', 0))} / {CONFIG.max_loss_streak} 次",
            f"冷静期剩余：{remaining / 3600:.1f} 小时",
            f"纪律状态：{'冷静期，禁止开仓' if locked else '正常观察'}",
        ]
    )


def record_trade(result: str) -> bool:
    refresh_day()
    result = (result or "").strip().lower()

    if result == "loss":
        state["daily_losses"] = int(state.get("daily_losses", 0)) + 1
        state["loss_streak"] = int(state.get("loss_streak", 0)) + 1
    elif result in ("win", "profit"):
        state["loss_streak"] = 0
    else:
        safe_print("请输入 win 或 loss")
        return False

    if (
        int(state["daily_losses"]) >= CONFIG.max_daily_losses
        or int(state["loss_streak"]) >= CONFIG.max_loss_streak
    ):
        state["cooldown_until"] = max(
            float(state.get("cooldown_until", 0.0) or 0.0),
            time.time() + CONFIG.cooling_hours * 3600,
        )

    save_state()
    safe_print(discipline_status())
    return True


# ==========================================
# 3. 错误分类 / 网络配置
# ==========================================


class OkxError(RuntimeError):
    """OKX 业务错误或不可重试的请求错误。"""

    def __init__(self, message: str, code: str | None = None, retryable: bool = False):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


def describe_exc(exc: BaseException) -> str:
    """把底层异常翻译成人能看懂的一句话。"""
    if isinstance(exc, OkxError):
        return str(exc)

    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "请求超时（网络不通 / 需要代理 / OKX 在该网络下不可达）"

    cert_error = getattr(aiohttp, "ClientConnectorCertificateError", ())
    proxy_error = getattr(aiohttp, "ClientProxyConnectionError", ())
    content_type_error = getattr(aiohttp, "ContentTypeError", ())

    if isinstance(exc, cert_error):
        return f"TLS 证书校验失败（公司代理/杀毒软件中间人证书？可先用 --insecure 排查）：{exc}"
    if isinstance(exc, proxy_error):
        return f"代理连接失败（代理没开或端口不对）：{exc}"
    if isinstance(exc, aiohttp.ClientConnectorError):
        return f"TCP/TLS 连接失败（DNS、被墙、端口被拦）：{exc}"
    if isinstance(exc, aiohttp.ServerDisconnectedError):
        return f"服务器提前断开连接：{exc}"
    if isinstance(exc, content_type_error):
        return f"响应不是 JSON（多半被 WAF / 代理拦截并返回了 HTML）：{exc}"
    if isinstance(exc, aiohttp.ClientResponseError):
        return f"HTTP {exc.status} {exc.message}"
    if isinstance(exc, aiohttp.ClientError):
        return f"HTTP 客户端错误 {type(exc).__name__}: {exc}"
    if isinstance(exc, WebSocketException):
        return f"WebSocket 错误 {type(exc).__name__}: {exc}"
    if isinstance(exc, socket.gaierror):
        return f"DNS 解析失败：{exc}"
    return f"{type(exc).__name__}: {exc}"


def detect_proxy(explicit: str | None) -> tuple[str | None, str]:
    """返回 (代理地址, 来源说明)。

    优先级：显式参数 > 环境变量 > Windows 系统代理(WinINET/注册表) > 直连。

    这正是 ""requests 能跑、aiohttp 跑不动"" 最常见的根因：
    requests 默认 trust_env=True，会读 HTTP(S)_PROXY 和 Windows 系统代理；
    aiohttp 的 ClientSession 默认 trust_env=False，会直接裸连，
    在国内网络下裸连 OKX 往往超时或被 reset。
    """
    if explicit:
        value = explicit.strip()
        if value.lower() in ("none", "off", "direct", "no"):
            return None, "命令行显式直连"
        if value.lower() != "auto":
            return value, "命令行显式指定"

    for key in ("HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy", "HTTP_PROXY", "http_proxy"):
        value = os.getenv(key)
        if value:
            return value.strip(), f"环境变量 {key}"

    try:
        proxies = urllib.request.getproxies()
    except Exception:  # noqa: BLE001 - 读注册表失败不影响主流程
        proxies = {}

    for scheme in ("https", "http"):
        value = proxies.get(scheme)
        if value:
            return value.strip(), f"系统代理（注册表 {scheme}）"

    return None, "直连（未发现任何代理）"


def make_ssl_context() -> Any:
    if not CONFIG.insecure:
        return True
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    log.warning("已启用 --insecure：TLS 证书不再校验，仅用于排查问题")
    return context


def build_session() -> aiohttp.ClientSession:
    """统一 HTTP 出口。

    与 requests 对齐：
      * trust_env=True            -> 读 HTTP(S)_PROXY / NO_PROXY
      * 显式 proxy=CONFIG.proxy   -> 读到的系统代理（aiohttp 不会自动读 WinINET）
      * 统一 User-Agent / Accept  -> 避免被 WAF 按默认 aiohttp UA 拦截
      * connect/sock_read 分开   -> 连接慢和响应慢能区分开
    """
    connector = aiohttp.TCPConnector(
        limit=32,
        ttl_dns_cache=300,
        ssl=make_ssl_context(),
    )
    kwargs: dict[str, Any] = {
        "connector": connector,
        "trust_env": True,
        "headers": DEFAULT_HEADERS,
        "timeout": aiohttp.ClientTimeout(
            total=CONFIG.http_timeout,
            connect=min(15.0, CONFIG.http_timeout),
            sock_read=CONFIG.http_timeout,
        ),
    }
    if CONFIG.proxy:
        kwargs["proxy"] = CONFIG.proxy
    return aiohttp.ClientSession(**kwargs)


# ==========================================
# 4. OKX REST（只读）
# ==========================================


async def okx_get_json(
    session: aiohttp.ClientSession,
    path: str,
    params: dict | None = None,
    *,
    retries: int = 3,
    timeout: float | None = None,
    tag: str = "",
) -> list:
    """GET 一个只读 OKX 接口并返回 data 列表，带分类重试。"""
    if not path.startswith(READ_ONLY_PREFIXES):
        # 硬性边界：这里以后被人加了 /api/v5/trade/... 会立刻炸掉
        raise OkxError(f"拒绝请求非只读接口：{path}")

    url = CONFIG.rest_base.rstrip("/") + path
    label = tag or path
    last_error: BaseException | None = None

    for attempt in range(1, retries + 1):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout or CONFIG.http_timeout),
            ) as response:
                body = await response.text()
                status = response.status

            if status >= 400:
                raise OkxError(
                    f"HTTP {status}，响应前 200 字符：{body[:200]!r}",
                    code=str(status),
                    retryable=status in (408, 425, 429) or status >= 500,
                )

            try:
                payload = json.loads(body)
            except ValueError as exc:
                raise OkxError(
                    f"响应不是 JSON（前 200 字符）：{body[:200]!r}", retryable=True
                ) from exc

            if not isinstance(payload, dict):
                raise OkxError("响应结构异常：顶层不是 JSON 对象", retryable=True)

            code = str(payload.get("code", ""))
            if code != "0":
                raise OkxError(
                    f"OKX 业务错误 code={code} msg={payload.get('msg')!r}",
                    code=code,
                    retryable=code in RETRYABLE_CODES,
                )

            data = payload.get("data")
            if data is None:
                return []
            if not isinstance(data, list):
                raise OkxError(f"data 字段不是列表：{type(data).__name__}")
            return data

        except asyncio.CancelledError:
            raise
        except OkxError as exc:
            last_error = exc
            if not exc.retryable or attempt >= retries:
                log.error("[%s] 失败（第 %d/%d 次）：%s", label, attempt, retries, exc)
                raise
            log.warning("[%s] 第 %d/%d 次失败，稍后重试：%s", label, attempt, retries, exc)
        except Exception as exc:  # noqa: BLE001 - 网络异常种类太多，统一记录
            last_error = exc
            if attempt >= retries:
                log.error(
                    "[%s] 失败（第 %d/%d 次）：%s",
                    label,
                    attempt,
                    retries,
                    describe_exc(exc),
                )
                log.debug("[%s] 异常详情", label, exc_info=True)
                raise
            log.warning(
                "[%s] 第 %d/%d 次异常，稍后重试：%s",
                label,
                attempt,
                retries,
                describe_exc(exc),
            )
            log.debug("[%s] 异常详情", label, exc_info=True)

        await asyncio.sleep(min(1.5 * (2 ** (attempt - 1)), 8.0) + random.uniform(0, 0.4))

    raise OkxError(f"{label} 请求失败：{describe_exc(last_error or RuntimeError('未知错误'))}")


async def fetch_swap_instruments(session: aiohttp.ClientSession) -> dict[str, dict]:
    rows = await okx_get_json(
        session,
        "/api/v5/public/instruments",
        {"instType": "SWAP"},
        retries=3,
        tag="合约列表",
    )
    meta: dict[str, dict] = {}
    for row in rows:
        if isinstance(row, dict) and row.get("instId"):
            meta[row["instId"]] = row
    if not meta:
        raise OkxError("合约列表为空，接口返回异常")
    return meta


def check_symbols(
    meta: dict[str, dict], symbols: Sequence[str]
) -> tuple[list[str], list[str]]:
    """返回 (可用, 不可用)。不可用时给出近似合约名，方便判断是不是写错了。"""
    available: list[str] = []
    missing: list[str] = []

    for symbol in symbols:
        info = meta.get(symbol)
        if info is None:
            missing.append(symbol)
            base = symbol.split("-")[0].upper()
            similar = sorted(k for k in meta if base in k.upper())[:5]
            hint = f"，近似合约：{', '.join(similar)}" if similar else "，OKX 上没有相近合约"
            log.error("合约不存在：%s%s", symbol, hint)
            continue
        state_value = str(info.get("state", "live"))
        if state_value != "live":
            log.warning("合约 %s 当前状态为 %s（可能暂停交易或已下架）", symbol, state_value)
        available.append(symbol)

    return available, missing


async def fetch_candles(
    session: aiohttp.ClientSession,
    inst_id: str,
    bar: str,
    *,
    limit: int | None = None,
    pages: int = 1,
) -> list:
    """拉取 K线。OKX 返回按时间倒序，翻页用 after（取更早的数据）。"""
    collected: list = []
    before: int | None = None
    limit = limit or CONFIG.candle_limit

    for page in range(1, max(1, pages) + 1):
        params = {"instId": inst_id, "bar": bar, "limit": str(limit)}
        if before is not None:
            params["after"] = str(before)

        rows = await okx_get_json(
            session,
            "/api/v5/market/candles",
            params,
            retries=3,
            tag=f"{inst_id} {bar} 第{page}页",
        )
        if not rows:
            break
        collected.extend(rows)

        timestamps = []
        for row in rows:
            try:
                timestamps.append(int(float(row[0])))
            except (TypeError, ValueError, IndexError):
                continue
        if not timestamps:
            break
        before = min(timestamps)

    return collected


def parse_candle(row: Any) -> dict | None:
    """把一行 K线转成 dict。

    OKX 字段顺序：[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
    confirm == "1" 表示这根K线已经收盘（历史接口里也可能给数字 1）。
    """
    if not isinstance(row, (list, tuple)) or len(row) < 6:
        log.debug("跳过异常K线行：%r", row)
        return None

    try:
        ts = int(float(row[0]))
        open_ = float(row[1])
        high = float(row[2])
        low = float(row[3])
        close = float(row[4])
    except (TypeError, ValueError):
        log.debug("K线字段无法转数字：%r", row)
        return None

    if ts <= 0 or min(open_, high, low, close) <= 0:
        log.debug("K线价格/时间非法：%r", row)
        return None

    if high < max(open_, close) or low > min(open_, close):
        # 交易所偶尔给出不自洽的极值，修正而不是丢弃
        log.debug("K线 OHLC 不自洽，已修正：%r", row)
        high = max(high, open_, close)
        low = min(low, open_, close)

    confirmed = False
    if len(row) >= 9:
        confirmed = str(row[8]).strip() in ("1", "True", "true")

    return {
        "time": ts,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "closed": confirmed,
    }


def parse_candles(rows: Iterable[Any], *, only_closed: bool = True) -> list[dict]:
    """解析 + 去重 + 按时间升序。"""
    unique: dict[int, dict] = {}
    for row in rows or []:
        candle = parse_candle(row)
        if candle is None:
            continue
        if only_closed and not candle["closed"]:
            continue
        unique[candle["time"]] = candle
    return [unique[ts] for ts in sorted(unique)]


# 历史数据：(inst_id, interval) -> deque[dict]
history: dict[tuple[str, str], deque] = {}
last_closed: dict[tuple[str, str], int] = {}
last_alert: dict[tuple[str, str, str], tuple[float, float]] = {}
# 已经处理过的收盘K线时间戳，独立于 history 保存：
# 重连补数据时 history 会被 REST 快照覆盖，而 REST 有时比 WS 慢半秒，
# 只靠 deque 去重会把同一根K线再提醒一次。
closed_seen: dict[tuple[str, str], deque] = {}

alerts: asyncio.Queue | None = None


def remember_closed(key: tuple[str, str], timestamp: int) -> None:
    seen = closed_seen.setdefault(key, deque(maxlen=500))
    if timestamp not in seen:
        seen.append(timestamp)


def enqueue_alert(message: str) -> None:
    """把提醒交给发送队列，绝不在行情回调里做网络 IO。"""
    if alerts is None:
        log.info("[提醒] %s", message.replace("\n", " | ")[:400])
        return
    try:
        alerts.put_nowait(message)
    except asyncio.QueueFull:
        log.error("提醒队列已满，丢弃一条：%s", message.splitlines()[0][:80])


async def notify_worker(session: aiohttp.ClientSession) -> None:
    assert alerts is not None
    while True:
        message = await alerts.get()
        try:
            await notify(session, message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 单条推送失败不能停掉队列
            log.error("提醒发送失败：%s", describe_exc(exc))
        finally:
            alerts.task_done()


async def load_history(
    session: aiohttp.ClientSession,
    inst_id: str,
    interval: str,
    *,
    attempts: int = 2,
) -> deque:
    """加载历史K线；失败抛 RuntimeError，由上层决定是跳过还是退出。"""
    key = (inst_id, interval)
    last_error: BaseException | None = None

    for attempt in range(1, attempts + 1):
        try:
            rows = await fetch_candles(session, inst_id, interval, limit=CONFIG.candle_limit)
            bars = parse_candles(rows, only_closed=True)

            if len(bars) < MIN_BARS:
                raise RuntimeError(
                    f"已收盘K线只有 {len(bars)} 根（需要 >= {MIN_BARS} 根，"
                    f"接口共返回 {len(rows)} 行）"
                )

            data = deque(bars[-CONFIG.max_bars:], maxlen=CONFIG.max_bars)
            history[key] = data
            last_closed[key] = data[-1]["time"]
            for bar in data:
                remember_closed(key, bar["time"])
            log.info(
                "[初始化] %-18s %-3s 已收盘 %3d 根 | 最新 %s | 收盘 %s",
                inst_id,
                interval,
                len(data),
                fmt_ts(data[-1]["time"]),
                data[-1]["close"],
            )
            return data

        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 记录后重试
            last_error = exc
            log.warning(
                "[初始化 %d/%d] %s %s 失败：%s",
                attempt,
                attempts,
                inst_id,
                interval,
                describe_exc(exc),
            )
            log.debug("[初始化] 异常详情", exc_info=True)
            if attempt < attempts:
                await asyncio.sleep(min(2.0 * attempt, 6.0) + random.uniform(0, 0.5))

    raise RuntimeError(
        f"{inst_id} {interval} 初始化失败：{describe_exc(last_error or RuntimeError('未知错误'))}"
    ) from last_error


# ==========================================
# 5. 支撑阻力 / Pinbar
# ==========================================


def interval_ms(interval: str) -> int:
    unit = interval[-1:]
    try:
        number = int(interval[:-1])
    except ValueError:
        return 60_000
    multipliers = {
        "m": 60_000,
        "H": 3_600_000,
        "D": 86_400_000,
        "W": 604_800_000,
        "M": 2_592_000_000,
    }
    return number * multipliers.get(unit, 60_000)


def get_levels(inst_id: str, interval: str) -> tuple[float | None, float | None]:
    bars = history.get((inst_id, interval))
    if not bars or len(bars) < MIN_BARS:
        return None, None

    recent = list(bars)[-CONFIG.lookback:]
    support = min(bar["low"] for bar in recent)
    resistance = max(bar["high"] for bar in recent)

    if support <= 0 or resistance <= support:
        return None, None
    return support, resistance


def detect_pinbar(candle: dict) -> str | None:
    """经典 Pinbar：长影线 + 小实体，且收盘靠近影线另一端。"""
    open_ = candle["open"]
    high = candle["high"]
    low = candle["low"]
    close = candle["close"]

    total = high - low
    if total <= 0:
        return None

    body = abs(close - open_)
    body_ref = max(body, total * 0.01)
    upper = high - max(open_, close)
    lower = min(open_, close) - low

    if (
        lower >= body_ref * 2
        and upper <= body_ref
        and body <= total * 0.35
        and (close - low) / total >= 0.65
    ):
        return "看涨 Pinbar 🟢"

    if (
        upper >= body_ref * 2
        and lower <= body_ref
        and body <= total * 0.35
        and (high - close) / total >= 0.65
    ):
        return "看跌 Pinbar 🔴"

    return None


# ==========================================
# 6. 钉钉推送
# ==========================================


def build_dingtalk_url(
    webhook: str, secret: str, timestamp_ms: int | None = None
) -> str:
    """钉钉自定义机器人加签：sign = base64(hmac_sha256(secret, f"{ts}\\n{secret}"))。"""
    url = webhook
    if secret:
        timestamp = str(timestamp_ms if timestamp_ms is not None else int(time.time() * 1000))
        sign_text = f"{timestamp}\n{secret}"
        digest = hmac.new(
            secret.encode("utf-8"), sign_text.encode("utf-8"), hashlib.sha256
        ).digest()
        sign = base64.b64encode(digest).decode()
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}timestamp={timestamp}&sign={urllib.parse.quote_plus(sign)}"
    return url


async def notify(session: aiohttp.ClientSession, message: str) -> bool:
    """发送钉钉文本消息；失败重试一次，返回是否成功。"""
    if not CONFIG.ding_enabled:
        log.info("[钉钉已禁用] %s", message.replace("\n", " | ")[:400])
        return False

    if not CONFIG.ding_webhook:
        log.info("[钉钉未配置] %s", message.replace("\n", " | ")[:400])
        return False

    payload = {
        "msgtype": "text",
        "text": {"content": message[:4000]},
    }

    for attempt in (1, 2):
        url = build_dingtalk_url(CONFIG.ding_webhook, CONFIG.ding_secret)
        try:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                status = response.status
                body = await response.text()

            try:
                result = json.loads(body)
            except ValueError:
                log.error("钉钉返回非 JSON（HTTP %s）：%r", status, body[:200])
                return False

            if status == 200 and result.get("errcode") == 0:
                log.info("钉钉推送成功")
                return True

            log.error(
                "钉钉推送失败 HTTP=%s errcode=%s errmsg=%s",
                status,
                result.get("errcode"),
                result.get("errmsg"),
            )
            return False

        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 推送失败不能影响监控
            log.warning("钉钉推送异常（第 %d 次）：%s", attempt, describe_exc(exc))
            if attempt == 1:
                await asyncio.sleep(1.0)

    return False


def format_level_alert(
    inst_id: str,
    interval: str,
    label: str,
    price: float,
    level: float,
    distance: float,
) -> str:
    lock_note = ""
    if trading_locked():
        lock_note = "\n🚫 当前处于冷静期：只看，不开仓。"

    return (
        f"⚠️ Trading Guard {__version__}\n"
        f"标的：{inst_id}\n"
        f"周期：{interval}\n"
        f"状态：{label}\n"
        f"当前价格：{price:g}\n"
        f"关键价位：{level:g}\n"
        f"偏离比例：{distance:.3%}\n\n"
        f"{discipline_status()}{lock_note}\n"
        f"请先观察，不要冲动交易。"
    )


def format_pinbar_alert(
    inst_id: str,
    interval: str,
    pattern: str,
    candle: dict,
    support: float | None,
    resistance: float | None,
) -> str:
    lock_note = ""
    if trading_locked():
        lock_note = "\n🚫 当前处于冷静期：只看，不开仓。"

    levels = []
    if support is not None:
        levels.append(f"此前支撑：{support:g}")
    if resistance is not None:
        levels.append(f"此前阻力：{resistance:g}")

    body = "\n".join(
        [
            f"📊 Trading Guard 形态警报",
            f"标的：{inst_id}",
            f"周期：{interval}",
            f"形态：{pattern}",
            f"开盘：{candle['open']:g}",
            f"最高：{candle['high']:g}",
            f"最低：{candle['low']:g}",
            f"收盘：{candle['close']:g}",
            *levels,
        ]
    )
    return f"{body}\n\n{discipline_status()}{lock_note}\n仅供观察，不构成交易建议。"


# ==========================================
# 7. 接近关键位提醒
# ==========================================


def check_levels(inst_id: str, interval: str, price: float) -> None:
    """价格接近支撑/阻力时入队提醒（同方向 5 分钟冷却）。"""
    support, resistance = get_levels(inst_id, interval)
    if support is None or resistance is None or price <= 0:
        return

    checks = (
        ("support", support, "接近支撑位 🟢"),
        ("resistance", resistance, "接近阻力位 🔴"),
    )

    now = time.monotonic()
    for kind, level, label in checks:
        if level <= 0:
            continue

        distance = abs(price - level) / level
        if distance > CONFIG.touch_pct:
            continue

        key = (inst_id, interval, kind)
        previous_level, previous_at = last_alert.get(key, (0.0, 0.0))
        level_changed = previous_level <= 0 or abs(level - previous_level) / level > 0.001

        if now - previous_at < CONFIG.alert_cooldown and not level_changed:
            continue

        last_alert[key] = (level, now)
        message = format_level_alert(inst_id, interval, label, price, level, distance)
        log.info(
            "[接近] %s %s %s 价格=%g 关键位=%g 偏离=%.3f%%",
            inst_id,
            interval,
            label,
            price,
            level,
            distance * 100,
        )
        enqueue_alert(message)


# ==========================================
# 8. 实时K线处理（历史与实时的衔接）
# ==========================================


def handle_candle(arg: dict, row: Any) -> None:
    inst_id = str(arg.get("instId") or "")
    channel = str(arg.get("channel") or "")
    if not inst_id or not channel.startswith("candle"):
        return
    interval = channel[len("candle"):]

    key = (inst_id, interval)
    bars = history.get(key)
    if bars is None:
        # 没初始化过的频道（比如刚订阅成功但补数据失败）直接忽略
        return

    candle = parse_candle(row)
    if candle is None:
        return

    # 未收盘：只做价格提醒，不能进历史，否则支撑阻力会被当前价污染
    if not candle["closed"]:
        check_levels(inst_id, interval, candle["close"])
        return

    # 以"历史里最新的那根"为准：重连补数据后 last_closed 可能回退，
    # 只看 last_closed 会把同一根K线再处理一次
    newest = bars[-1]["time"] if bars else 0
    previous_time = max(last_closed.get(key, 0), newest)

    # 再加一层独立去重：REST 快照可能比 WS 慢半拍，补数据后这根K线不在 deque 里，
    # 但它确实已经提醒过了
    seen = closed_seen.get(key)
    if seen is not None and candle["time"] in seen:
        log.debug("[去重] %s %s 该K线已处理过 %s", inst_id, interval, fmt_ts(candle["time"]))
        return

    if candle["time"] <= previous_time:
        if candle["time"] == previous_time:
            log.debug("[去重] %s %s 重复收盘推送 %s", inst_id, interval, fmt_ts(candle["time"]))
        else:
            log.debug(
                "[过期] %s %s K线 %s 早于已记录的 %s",
                inst_id,
                interval,
                fmt_ts(candle["time"]),
                fmt_ts(previous_time),
            )
        return

    step = interval_ms(interval)
    if previous_time and candle["time"] - previous_time > step * 1.5:
        missing = (candle["time"] - previous_time) // step - 1
        log.warning(
            "[缺口] %s %s 缺失约 %d 根K线（%s -> %s）",
            inst_id,
            interval,
            max(1, missing),
            fmt_ts(previous_time),
            fmt_ts(candle["time"]),
        )

    # 先用"这根之前"的数据算关键位，再入库
    support, resistance = get_levels(inst_id, interval)
    pattern = detect_pinbar(candle)

    last_closed[key] = candle["time"]
    bars.append(candle)
    remember_closed(key, candle["time"])

    log.info(
        "[收盘] %-18s %-3s O=%g H=%g L=%g C=%g",
        inst_id,
        interval,
        candle["open"],
        candle["high"],
        candle["low"],
        candle["close"],
    )

    if pattern:
        log.info("[形态] %s %s %s", inst_id, interval, pattern)
        enqueue_alert(
            format_pinbar_alert(inst_id, interval, pattern, candle, support, resistance)
        )


# ==========================================
# 9. WebSocket
# ==========================================

WS_HAS_PROXY = "proxy" in inspect.signature(websockets.connect).parameters


class Link:
    """保存当前 WS 连接，供后台补订阅使用。"""

    def __init__(self) -> None:
        self.ws: Any = None
        self.lock = asyncio.Lock()
        self.subscribed: set[tuple[str, str]] = set()

    async def send_text(self, text: str) -> bool:
        ws = self.ws
        if ws is None:
            return False
        try:
            await ws.send(text)
            return True
        except Exception as exc:  # noqa: BLE001 - 连接刚好断开属正常
            log.debug("WS 发送失败：%s", describe_exc(exc))
            return False

    async def subscribe(self, keys: Sequence[tuple[str, str]]) -> bool:
        pending = [key for key in keys if key not in self.subscribed]
        if not pending:
            return False
        async with self.lock:
            if self.ws is None:
                return False
            payload = {"op": "subscribe", "args": build_subscriptions(pending)}
            await self.ws.send(json.dumps(payload, ensure_ascii=False))
            self.subscribed.update(pending)
            log.info("WS 追加订阅 %d 个频道：%s", len(pending), ", ".join(f"{i} {b}" for i, b in pending))
            return True


def build_subscriptions(keys: Iterable[tuple[str, str]]) -> list[dict]:
    return [{"channel": f"candle{interval}", "instId": inst_id} for inst_id, interval in keys]


async def ws_connect() -> Any:
    kwargs: dict[str, Any] = {
        "open_timeout": 15,
        # OKX 用文本 "ping"/"pong" 心跳，关掉库自己的 ping 帧，避免和服务端不兼容
        "ping_interval": None,
        "ping_timeout": None,
        "close_timeout": 5,
        "max_queue": 256,
        "max_size": 4 * 1024 * 1024,
    }
    if WS_HAS_PROXY:
        # websockets>=15 默认 proxy=True（读环境变量），这里和 REST 用同一个出口
        kwargs["proxy"] = CONFIG.proxy
    if CONFIG.insecure:
        kwargs["ssl"] = make_ssl_context()
    return await websockets.connect(CONFIG.ws_url, **kwargs)


async def handle_ws_message(session: aiohttp.ClientSession, link: Link, raw: Any) -> None:
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "replace")
    text = str(raw).strip()

    if text == "ping":
        await link.send_text("pong")
        log.debug("收到服务端 ping，已回 pong")
        return
    if text == "pong":
        log.debug("收到服务端 pong")
        return

    try:
        packet = json.loads(text)
    except ValueError:
        log.warning("WS 收到无法解析的消息：%r", text[:200])
        return

    if not isinstance(packet, dict):
        return

    event = packet.get("event")
    if event == "error":
        log.error(
            "WS 订阅错误 code=%s msg=%s arg=%s",
            packet.get("code"),
            packet.get("msg"),
            packet.get("arg"),
        )
        return
    if event in ("subscribe", "unsubscribe"):
        log.info("WS 订阅回执：%s", packet.get("arg"))
        return
    if event:
        log.debug("WS 事件 %s：%s", event, text[:200])
        return

    arg = packet.get("arg") or {}
    if not isinstance(arg, dict):
        return

    for row in packet.get("data") or []:
        handle_candle(arg, row)


async def resync(session: aiohttp.ClientSession, keys: Sequence[tuple[str, str]], reason: str) -> None:
    for inst_id, interval in list(keys):
        try:
            await load_history(session, inst_id, interval, attempts=2)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 单个失败保留旧数据继续跑
            log.warning("[%s] %s %s 补数据失败：%s", reason, inst_id, interval, describe_exc(exc))


async def ws_loop(
    session: aiohttp.ClientSession | None,
    link: Link,
    enabled: list[tuple[str, str]],
    *,
    initial_backoff: float = 3.0,
) -> None:
    attempt = 0

    while True:
        ws = None
        connected_at = 0.0
        try:
            log.info("正在连接 OKX WebSocket：%s", CONFIG.ws_url)
            ws = await ws_connect()
            link.ws = ws
            link.subscribed.clear()

            await ws.send(
                json.dumps(
                    {"op": "subscribe", "args": build_subscriptions(enabled)},
                    ensure_ascii=False,
                )
            )
            link.subscribed.update(enabled)
            log.info("✅ WebSocket 已连接，已订阅 %d 个频道", len(enabled))
            connected_at = time.monotonic()

            while True:
                # 看门狗：服务端每 30s 会发文本 ping，90s 一条消息都没有 = 连接已死
                raw = await asyncio.wait_for(ws.recv(), timeout=CONFIG.idle_timeout)
                await handle_ws_message(session, link, raw)

        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            log.warning(
                "WebSocket 已 %.0f 秒没有任何消息，判定连接失效，主动重连",
                CONFIG.idle_timeout,
            )
        except ConnectionClosed as exc:
            log.warning(
                "WebSocket 断开：code=%s reason=%s",
                getattr(exc, "code", None),
                getattr(exc, "reason", "") or "",
            )
        except Exception as exc:  # noqa: BLE001 - 任何异常都要能重连
            log.error("WebSocket 运行异常：%s", describe_exc(exc))
            log.debug("WebSocket 异常详情", exc_info=True)
        finally:
            link.ws = None
            link.subscribed.clear()
            if ws is not None:
                try:
                    await asyncio.wait_for(ws.close(), timeout=5)
                except Exception:  # noqa: BLE001 - 关闭失败无所谓
                    pass

        # 稳定运行 30s 以上才把退避清零，避免"连上就断"时疯狂重连
        if connected_at and time.monotonic() - connected_at >= 30:
            attempt = 0

        attempt += 1
        delay = min(initial_backoff * (2 ** (attempt - 1)), 60.0) + random.uniform(0, 1.0)
        log.info("🔁 %.1f 秒后重连（第 %d 次）", delay, attempt)
        await asyncio.sleep(delay)

        # 重连前先把断线期间可能错过的K线补齐，再订阅
        # session 为 None 只出现在离线自测里，跳过补数据
        if session is not None:
            await resync(session, enabled, "重连补数据")


# ==========================================
# 10. 监控主流程
# ==========================================


async def init_market_data(
    session: aiohttp.ClientSession,
    symbols: Sequence[str],
    intervals: Sequence[str],
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """逐个初始化；单个失败只记录，不拖垮其它交易对。"""
    enabled: list[tuple[str, str]] = []
    failed: list[tuple[str, str]] = []

    for inst_id in symbols:
        for interval in intervals:
            try:
                await load_history(session, inst_id, interval)
                enabled.append((inst_id, interval))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 这里是隔离边界
                log.error("✗ %s %s 初始化失败：%s", inst_id, interval, exc)
                failed.append((inst_id, interval))

    return enabled, failed


async def recovery_loop(
    session: aiohttp.ClientSession,
    link: Link,
    enabled: list[tuple[str, str]],
    failed: set[tuple[str, str]],
) -> None:
    """每 5 分钟重试一次失败/缺失的组合，成功后自动追加订阅。"""
    while True:
        await asyncio.sleep(300)
        if not failed:
            continue

        for key in list(failed):
            inst_id, interval = key
            try:
                await load_history(session, inst_id, interval, attempts=1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 仍然不可用就下次再试
                log.info("[补数据] %s %s 仍不可用：%s", inst_id, interval, describe_exc(exc))
                continue

            failed.discard(key)
            if key not in enabled:
                enabled.append(key)
            log.info("[补数据] %s %s 已恢复，追加订阅", inst_id, interval)
            try:
                await link.subscribe([key])
            except Exception as exc:  # noqa: BLE001 - 重连后会自动补订阅
                log.warning("[补数据] %s %s 追加订阅失败：%s", inst_id, interval, describe_exc(exc))


def startup_message(enabled: Sequence[tuple[str, str]], failed: Sequence[tuple[str, str]]) -> str:
    symbol_set = sorted({inst for inst, _ in enabled})
    ready_intervals = {iv for _, iv in enabled}
    interval_set = [iv for iv in CONFIG.intervals if iv in ready_intervals]
    lines = [
        f"🛡 Trading Guard {__version__} 已启动",
        "行情源：OKX REST + WebSocket",
        f"合约：{', '.join(symbol_set)}",
        f"周期：{' + '.join(interval_set)}",
        "支撑阻力：开启",
        "Pinbar识别：开启",
        "交易纪律：开启",
    ]
    if failed:
        lines.append(f"⚠️ 未就绪：{', '.join(f'{i} {b}' for i, b in failed)}")
    return "\n".join(lines) + "\n\n" + discipline_status()


async def preflight(session: aiohttp.ClientSession) -> bool:
    """开跑前先打一枪，避免网络不通时 8 个组合各重试 3 次、卡好几分钟。"""
    try:
        data = await okx_get_json(
            session, "/api/v5/public/time", retries=2, timeout=10, tag="连通性预检"
        )
        if not data:
            raise OkxError("预检接口返回空数据")
        log.info("REST 连通性预检通过")
        return True
    except Exception as exc:  # noqa: BLE001 - 这里就是要吞掉并给出人话提示
        log.critical("OKX REST 接口不可用：%s", describe_exc(exc))
        log.critical("请先运行：py trading_guard.py diag")
        log.critical(
            "如果本机需要代理：py trading_guard.py --proxy auto "
            "或 --proxy http://127.0.0.1:7897"
        )
        return False


async def monitor() -> int:
    global alerts

    alerts = asyncio.Queue(maxsize=256)

    log.info(
        "Trading Guard %s 启动 | 代理：%s（%s）",
        __version__,
        CONFIG.proxy or "无",
        CONFIG.proxy_source,
    )
    if not CONFIG.ding_webhook:
        log.warning("未检测到 DING_WEBHOOK，钉钉推送会被跳过（只打印到控制台）")

    async with build_session() as session:
        if not await preflight(session):
            return 2

        # 1) 合约可用性校验（失败不致命，只降级为警告）
        symbols: list[str] = list(CONFIG.symbols)
        try:
            meta = await fetch_swap_instruments(session)
            available, missing = check_symbols(meta, symbols)
            if missing:
                log.error("以下合约在 OKX 不可用，将被跳过：%s", ", ".join(missing))
            symbols = available
        except Exception as exc:  # noqa: BLE001 - 校验失败不阻塞行情
            log.warning("合约校验失败，跳过校验直接拉K线：%s", describe_exc(exc))

        if not symbols:
            log.critical("没有任何可用合约，退出。请先运行：py trading_guard.py verify")
            return 2

        # 2) 历史K线（逐个隔离）
        enabled, failed = await init_market_data(session, symbols, CONFIG.intervals)

        if not enabled:
            log.critical("所有交易对的K线都初始化失败，退出。")
            log.critical("请先运行：py trading_guard.py diag")
            return 2

        if failed:
            log.warning(
                "有 %d 个组合未就绪，程序会继续运行并在后台重试：%s",
                len(failed),
                ", ".join(f"{i} {b}" for i, b in failed),
            )

        notifier = asyncio.create_task(notify_worker(session), name="notify")
        link = Link()
        tasks: list[asyncio.Task] = [notifier]

        failed_set = set(failed)
        if failed_set:
            tasks.append(
                asyncio.create_task(
                    recovery_loop(session, link, enabled, failed_set), name="recovery"
                )
            )

        await alerts.put(startup_message(enabled, failed))

        ws_task = asyncio.create_task(ws_loop(session, link, enabled), name="ws")

        try:
            if CONFIG.duration > 0:
                try:
                    await asyncio.wait_for(ws_task, timeout=CONFIG.duration)
                except asyncio.TimeoutError:
                    log.info("已达到 --duration %.0f 秒，正常退出", CONFIG.duration)
            else:
                await ws_task
        finally:
            for task in [*tasks, ws_task]:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, ws_task, return_exceptions=True)
            if CONFIG.notify_stop:
                try:
                    await notify(session, f"🛡 Trading Guard {__version__} 已停止")
                except Exception as exc:  # noqa: BLE001 - 退出路径不能抛
                    log.debug("停止通知发送失败：%s", exc)

    return 0


# ==========================================
# 11. 校验 / 诊断 / 自测
# ==========================================


async def cmd_verify(_: argparse.Namespace) -> int:
    async with build_session() as session:
        try:
            meta = await fetch_swap_instruments(session)
        except Exception as exc:  # noqa: BLE001
            log.error("拿不到合约列表：%s", describe_exc(exc))
            return 2

        available, missing = check_symbols(meta, CONFIG.symbols)

        safe_print(f"\n{'instId':<20}{'state':<10}{'ctVal':>12}{'上市时间':>22}")
        safe_print("-" * 66)
        for symbol in CONFIG.symbols:
            info = meta.get(symbol)
            if not info:
                safe_print(f"{symbol:<20}{'NOT FOUND':<10}")
                continue
            listed = info.get("listTime")
            listed_text = fmt_ts(int(listed)) if str(listed or "").isdigit() else "-"
            safe_print(
                f"{symbol:<20}{str(info.get('state')):<10}{str(info.get('ctVal')):>12}{listed_text:>22}"
            )

        safe_print("")
        ok = 0
        for symbol in available:
            for interval in CONFIG.intervals:
                try:
                    rows = await fetch_candles(
                        session, symbol, interval, limit=10, pages=1
                    )
                    parsed = parse_candles(rows, only_closed=False)
                    closed = sum(1 for c in parsed if c["closed"])
                    if parsed:
                        status = "OK"
                        ok += 1
                    else:
                        status = "空数据"
                    safe_print(
                        f"[{status:<4}] {symbol:<20}{interval:<4} 返回 {len(rows):>3} 行，"
                        f"可解析 {len(parsed):>3} 根（已收盘 {closed}）"
                    )
                except Exception as exc:  # noqa: BLE001
                    safe_print(f"[FAIL] {symbol:<20}{interval:<4} {describe_exc(exc)}")

        safe_print("")
        total_checks = max(1, len(available) * len(CONFIG.intervals))
        safe_print(f"合约可用 {len(available)}/{len(CONFIG.symbols)}，K线可用 {ok}/{total_checks}")

        if missing:
            safe_print(f"不可用合约：{', '.join(missing)}")
            return 1
        return 0


async def cmd_diag(_: argparse.Namespace) -> int:
    """把"为什么 requests 行、aiohttp 不行"拆成 6 步单独验证。"""
    parsed_rest = urllib.parse.urlparse(CONFIG.rest_base)
    rest_host = parsed_rest.hostname or "www.okx.com"
    use_tls = parsed_rest.scheme != "http"
    rest_port = parsed_rest.port or (443 if use_tls else 80)
    results: dict[str, bool] = {}

    def report(step: str, ok: bool, detail: str = "") -> None:
        results[step] = ok
        safe_print(f"[{' OK ' if ok else 'FAIL'}] {step:<12} {detail}")

    safe_print("=== Trading Guard 诊断 ===")
    safe_print(f"Python      : {sys.version.split()[0]} ({sys.executable})")
    safe_print(f"aiohttp     : {aiohttp.__version__}")
    safe_print(f"websockets  : {websockets.__version__}")
    try:
        import requests  # noqa: F401  仅用于对照

        safe_print(f"requests    : {requests.__version__}（仅用于对照，程序本身不依赖）")
    except ImportError:
        safe_print("requests    : 未安装（不影响运行）")
    safe_print("")

    try:
        proxies = urllib.request.getproxies()
    except Exception:  # noqa: BLE001
        proxies = {}
    report(
        "代理",
        True,
        f"{CONFIG.proxy or '直连'}（来源：{CONFIG.proxy_source}）| urllib.getproxies()={proxies}",
    )

    # 1) DNS
    infos: list = []
    try:
        infos = await asyncio.to_thread(
            socket.getaddrinfo, rest_host, rest_port, socket.AF_UNSPEC, socket.SOCK_STREAM
        )
        ips = sorted({info[4][0] for info in infos})
        report("DNS", bool(ips), f"{rest_host} -> {', '.join(ips[:4])}")
    except Exception as exc:  # noqa: BLE001
        report("DNS", False, f"{rest_host} 解析失败：{exc}")

    # 2) TCP
    ip = None
    if infos:
        ip = infos[0][4][0]
        try:
            start = time.perf_counter()
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ip, rest_port), timeout=8
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001
                pass
            report(
                f"TCP {rest_port}",
                True,
                f"{ip}:{rest_port} 建连成功 {(time.perf_counter() - start) * 1000:.0f} ms",
            )
        except Exception as exc:  # noqa: BLE001
            report(f"TCP {rest_port}", False, f"{ip}:{rest_port} 建连失败：{type(exc).__name__}: {exc}")

    # 3) TLS
    if not use_tls:
        report("TLS", True, f"{parsed_rest.scheme} 地址，跳过 TLS 检查")
    else:
        try:
            context = ssl.create_default_context()
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(rest_host, rest_port, ssl=context, server_hostname=rest_host),
                timeout=10,
            )
            cipher = writer.get_extra_info("cipher")
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001
                pass
            report("TLS", True, f"握手成功 {cipher[0] if cipher else ''}")
        except Exception as exc:  # noqa: BLE001
            report("TLS", False, f"{type(exc).__name__}: {exc}")

    # 4) aiohttp（程序实际使用的通道）
    aiohttp_ok = False
    try:
        start = time.perf_counter()
        async with build_session() as session:
            data = await okx_get_json(
                session, "/api/v5/public/time", retries=1, timeout=15, tag="诊断时间接口"
            )
        aiohttp_ok = bool(data)
        report("aiohttp", aiohttp_ok, f"/api/v5/public/time OK {(time.perf_counter() - start) * 1000:.0f} ms")
    except Exception as exc:  # noqa: BLE001
        report("aiohttp", False, describe_exc(exc))

    # 5) requests 对照
    try:
        import requests as requests_module

        start = time.perf_counter()
        response = requests_module.get(CONFIG.rest_base.rstrip("/") + "/api/v5/public/time", timeout=15)
        report(
            "requests",
            response.status_code == 200,
            f"HTTP {response.status_code} {(time.perf_counter() - start) * 1000:.0f} ms（对照组）",
        )
    except ImportError:
        report("requests", True, "未安装，跳过对照")
    except Exception as exc:  # noqa: BLE001
        report("requests", False, f"{type(exc).__name__}: {exc}（对照组）")

    # 6) 合约 + K线
    instruments_ok = False
    candles_ok = 0
    try:
        async with build_session() as session:
            meta = await fetch_swap_instruments(session)
            available, missing = check_symbols(meta, CONFIG.symbols)
            instruments_ok = not missing
            report("合约", instruments_ok, f"可用 {len(available)}/{len(CONFIG.symbols)}"
                   + (f"，缺失 {', '.join(missing)}" if missing else ""))
            for symbol in available:
                for interval in CONFIG.intervals:
                    try:
                        rows = await fetch_candles(session, symbol, interval, limit=5)
                        count = len(parse_candles(rows, only_closed=False))
                        candles_ok += 1 if count else 0
                        safe_print(f"        {symbol} {interval}: {len(rows)} 行 / {count} 根")
                    except Exception as exc:  # noqa: BLE001
                        safe_print(f"        {symbol} {interval}: {describe_exc(exc)}")
    except Exception as exc:  # noqa: BLE001
        report("合约", False, describe_exc(exc))

    # 7) WebSocket
    ws_ok = False
    try:
        ws = await asyncio.wait_for(ws_connect(), timeout=20)
        try:
            await ws.send(
                json.dumps({"op": "subscribe", "args": [{"channel": "candle5m", "instId": f"{CONFIG.symbols[0]}"}]})
            )
            got = False
            deadline = time.monotonic() + 12
            while time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.monotonic())
                except asyncio.TimeoutError:
                    break
                text = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
                if text.strip() == "ping":
                    await ws.send("pong")
                if '"event":"subscribe"' in text.replace(" ", "") or '"arg"' in text:
                    got = True
                    break
            ws_ok = got
            report("WebSocket", ws_ok, f"{CONFIG.ws_url} 订阅回执 {'已收到' if got else '未收到（12s 超时）'}")
        finally:
            await ws.close()
    except Exception as exc:  # noqa: BLE001
        report("WebSocket", False, describe_exc(exc))

    # 结论
    tcp_key = f"TCP {rest_port}"
    safe_print("\n=== 结论 ===")
    if results.get("TLS") is False or results.get(tcp_key) is False:
        safe_print("* 到 OKX 的 TCP/TLS 都不通：这是网络出口问题，requests 也不会成功。")
        safe_print("  需要开代理（Clash/V2Ray）后运行：py trading_guard.py --proxy auto")
    elif results.get("aiohttp") is False and results.get("requests") is True:
        safe_print("* requests 通、aiohttp 不通：几乎可以确定是代理没被 aiohttp 用上。")
        safe_print("  aiohttp 默认 trust_env=False，不读 HTTP(S)_PROXY 和 Windows 系统代理。")
        safe_print("  本版本已修复：显式读取系统代理。若仍失败，试：")
        safe_print("  py trading_guard.py diag --proxy http://127.0.0.1:7897")
    elif results.get("aiohttp") and not instruments_ok:
        safe_print("* 网络通但合约缺失：检查 instId 拼写；SOXL 这类股票永续在 OKX 属于 RWA 合约。")
    elif results.get("aiohttp") and ws_ok and candles_ok:
        safe_print("* 全链路正常。可以直接运行：py trading_guard.py")
    else:
        safe_print("* 请把上面 FAIL 的行贴出来，按提示逐项排查。")

    return 0 if all(results.values()) else 1


async def _run_selftest() -> tuple[int, int]:
    """离线自测：纯函数 + 本地回环 REST/WS，不访问外网。"""
    from aiohttp import web

    passed = 0
    failed = 0

    def check(name: str, fn) -> None:
        nonlocal passed, failed
        try:
            fn()
            passed += 1
            safe_print(f"  ✓ {name}")
        except AssertionError as exc:
            failed += 1
            safe_print(f"  ✗ {name}：{exc}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            safe_print(f"  ✗ {name}：{type(exc).__name__}: {exc}")

    safe_print("=== 离线自测 ===")

    check("parse_candle 正常行", lambda: _assert(
        parse_candle(["1700000000000", "100", "110", "95", "105", "1", "1", "1", "1"])["closed"] is True
    ))
    check("parse_candle 未收盘", lambda: _assert(
        parse_candle(["1700000000000", "100", "110", "95", "105", "1", "1", "1", "0"])["closed"] is False
    ))
    check("parse_candle 数字型 confirm", lambda: _assert(
        parse_candle(["1700000000000", "100", "110", "95", "105", "1", "1", "1", 1])["closed"] is True
    ))
    check("parse_candle 脏数据返回 None", lambda: _assert(
        parse_candle(["bad", "x", "y"]) is None and parse_candle(None) is None
    ))
    check("parse_candle 修正不自洽 OHLC", lambda: _assert(
        parse_candle(["1700000000000", "100", "99", "95", "105", "1", "1", "1", "1"])["high"] == 105
    ))
    check("parse_candles 排序去重", lambda: _assert(
        [c["time"] for c in parse_candles([
            ["3000", "1", "2", "1", "1.5", "", "", "", "1"],
            ["1000", "1", "2", "1", "1.5", "", "", "", "1"],
            ["3000", "1", "3", "1", "2.5", "", "", "", "1"],
        ])] == [1000, 3000]
    ))
    check("interval_ms", lambda: _assert(
        interval_ms("5m") == 300000 and interval_ms("15m") == 900000 and interval_ms("1H") == 3600000
    ))

    bullish = {"open": 100.0, "high": 101.0, "low": 90.0, "close": 100.8}
    bearish = {"open": 100.0, "high": 110.0, "low": 99.0, "close": 99.2}
    normal = {"open": 100.0, "high": 100.6, "low": 99.4, "close": 100.0}  # 十字星，两端影线对称
    check("Pinbar 看涨", lambda: _assert("看涨" in (detect_pinbar(bullish) or "")))
    check("Pinbar 看跌", lambda: _assert("看跌" in (detect_pinbar(bearish) or "")))
    check("Pinbar 普通K线不误报", lambda: _assert(detect_pinbar(normal) is None))
    check("Pinbar 零振幅保护", lambda: _assert(
        detect_pinbar({"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}) is None
    ))

    history[("TEST", "5m")] = deque(
        [{"time": i, "open": 100.0, "high": 100.0 + i % 5, "low": 90.0 - i % 3, "close": 100.0}
         for i in range(60)],
        maxlen=MAX_BARS,
    )
    support, resistance = get_levels("TEST", "5m")
    check("支撑阻力计算", lambda: _assert(
        support == min(b["low"] for b in list(history[("TEST", "5m")])[-LOOKBACK:])
        and resistance == max(b["high"] for b in list(history[("TEST", "5m")])[-LOOKBACK:])
    ))
    history.pop(("TEST", "5m"), None)

    url = build_dingtalk_url("https://oapi.dingtalk.com/robot/send?access_token=x", "secret", 1700000000000)

    def _sign_ok() -> bool:
        if "timestamp=1700000000000" not in url or "sign=" not in url:
            return False
        signature = urllib.parse.unquote_plus(url.split("sign=")[1])
        return len(base64.b64decode(signature)) == 32

    check("钉钉加签 URL", lambda: _assert(_sign_ok()))

    # 纪律状态机（用临时文件，不碰真实状态）
    original_state_file = CONFIG.state_file
    original_state = dict(state)
    CONFIG.state_file = Path(os.environ.get("TEMP", ".")) / "tg_selftest_state.json"
    try:
        if CONFIG.state_file.exists():
            CONFIG.state_file.unlink()

        globals()["state"] = _fresh_state()
        record_trade("loss")
        record_trade("loss")
        record_trade("loss")
        check("三次亏损触发冷静期", lambda: _assert(
            trading_locked() and state["cooldown_until"] > time.time()
        ))
        record_trade("win")
        check("盈利清零连亏", lambda: _assert(state["loss_streak"] == 0))
        check("状态文件已落盘", lambda: _assert(CONFIG.state_file.exists()))
    finally:
        if CONFIG.state_file.exists():
            CONFIG.state_file.unlink()
        CONFIG.state_file = original_state_file
        globals()["state"] = original_state

    # 回环 REST：验证 HTTP 层解析/分页/错误处理
    app = web.Application()

    async def _instruments(_request):
        return web.json_response({
            "code": "0",
            "msg": "",
            "data": [{"instId": "BTC-USDT-SWAP", "state": "live", "ctVal": "0.01", "listTime": "1600000000000"}],
        })

    async def _candles(_request):
        base = 1700000000000
        rows = [
            [str(base - index * 300000), "100", "101", "99", "100.5", "1", "1", "1", "1"]
            for index in range(100)
        ]
        # 混入一根未收盘 + 一行脏数据，验证过滤逻辑
        rows.insert(0, [str(base + 300000), "100", "101", "99", "100.5", "1", "1", "1", "0"])
        rows.append(["bad", "1", "2"])
        return web.json_response({"code": "0", "msg": "", "data": rows})

    async def _error(_request):
        return web.json_response({"code": "51001", "msg": "Instrument ID does not exist", "data": []})

    app.router.add_get("/api/v5/public/instruments", _instruments)
    app.router.add_get("/api/v5/market/candles", _candles)
    app.router.add_get("/api/v5/market/error", _error)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # noqa: SLF001 - 测试用

    original_base = CONFIG.rest_base
    CONFIG.rest_base = f"http://127.0.0.1:{port}"
    try:
        async with build_session() as session:
            bars = await load_history(session, "BTC-USDT-SWAP", "5m", attempts=1)
            check("回环 REST 加载历史", lambda: _assert(
                len(bars) == 100 and bars[-1]["time"] == 1700000000000 and bars[-1]["closed"]
            ))
            meta = await fetch_swap_instruments(session)
            check("回环 REST 合约列表", lambda: _assert("BTC-USDT-SWAP" in meta))

            async def _expect_error():
                try:
                    await okx_get_json(session, "/api/v5/market/error", retries=1)
                except OkxError as exc:
                    _assert("51001" in str(exc))
                    return
                raise AssertionError("错误码没有抛异常")

            await _expect_error()
            passed += 1
            safe_print("  ✓ OKX 业务错误码正确抛出")

            async def _expect_guard():
                try:
                    await okx_get_json(session, "/api/v5/trade/order", retries=1)
                except OkxError:
                    return
                raise AssertionError("只读白名单没有生效")

            await _expect_guard()
            passed += 1
            safe_print("  ✓ 非只读接口被拒绝")
    finally:
        CONFIG.rest_base = original_base
        await runner.cleanup()

    # 回环 WebSocket：订阅 / 文本 ping-pong / 收盘K线入库 / 断线重连
    received: list[str] = []
    connections = {"count": 0}

    async def ws_handler(websocket):
        connections["count"] += 1
        try:
            hello = json.loads(await asyncio.wait_for(websocket.recv(), timeout=5))
        except Exception:  # noqa: BLE001
            return
        assert hello.get("op") == "subscribe"
        await websocket.send(json.dumps({"event": "subscribe", "arg": hello["args"][0]}))
        for index in range(1, 4):
            await websocket.send(json.dumps({
                "arg": {"channel": "candle5m", "instId": "BTC-USDT-SWAP"},
                "data": [[str(1700000000000 + index * 300000), "100", "102", "99", "101", "1", "1", "1", "1"]],
            }))
        await websocket.send("ping")
        try:
            received.append(await asyncio.wait_for(websocket.recv(), timeout=5))
        except Exception:  # noqa: BLE001
            pass
        await websocket.close()

    ws_server = await websockets.serve(ws_handler, "127.0.0.1", 0)
    ws_port = ws_server.sockets[0].getsockname()[1]
    original_ws = CONFIG.ws_url
    CONFIG.ws_url = f"ws://127.0.0.1:{ws_port}"
    try:
        history[("BTC-USDT-SWAP", "5m")] = deque(maxlen=MAX_BARS)
        last_closed[("BTC-USDT-SWAP", "5m")] = 1700000000000
        link = Link()
        task = asyncio.create_task(
            ws_loop(None, link, [("BTC-USDT-SWAP", "5m")], initial_backoff=0.2)
        )
        await asyncio.sleep(4.0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        check("WS 订阅 + 收盘K线入库", lambda: _assert(
            len(history[("BTC-USDT-SWAP", "5m")]) == 3
        ))
        check("WS 文本 ping 回 pong", lambda: _assert(received and received[0] == "pong"))
        check("WS 断开后自动重连", lambda: _assert(connections["count"] >= 2))
    finally:
        CONFIG.ws_url = original_ws
        ws_server.close()
        await ws_server.wait_closed()
        history.pop(("BTC-USDT-SWAP", "5m"), None)
        last_closed.pop(("BTC-USDT-SWAP", "5m"), None)
        closed_seen.pop(("BTC-USDT-SWAP", "5m"), None)

    return passed, failed


def _assert(condition: bool, message: str = "断言失败") -> None:
    if not condition:
        raise AssertionError(message)


def cmd_selftest(_: argparse.Namespace) -> int:
    setup_logging("WARNING", None)
    passed, failed = asyncio.run(_run_selftest())
    safe_print(f"\n结果：{passed} 项通过，{failed} 项失败")
    return 0 if failed == 0 else 1


# ==========================================
# 12. 命令行
# ==========================================

COMMANDS = {
    "run",
    "status",
    "record-loss",
    "record-win",
    "reset-streak",
    "verify",
    "diag",
    "selftest",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trading_guard.py",
        description=f"Trading Guard {__version__} - OKX 只读行情监控",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbols", default=",".join(INSTRUMENTS), help="逗号分隔的交易对")
    parser.add_argument("--intervals", default=",".join(INTERVALS), help="逗号分隔的周期")
    parser.add_argument(
        "--proxy",
        default=None,
        help="auto（默认，读环境变量与 Windows 系统代理）/ none / http://127.0.0.1:7897",
    )
    parser.add_argument("--insecure", action="store_true", help="跳过 TLS 证书校验（仅排查用）")
    parser.add_argument("--rest-base", default=None, help="覆盖 OKX REST 地址")
    parser.add_argument("--ws-url", default=None, help="覆盖 OKX WebSocket 地址")
    parser.add_argument("--state-file", default=None, help="纪律状态文件路径")
    parser.add_argument("--log-file", default=None, help="日志文件路径（传 none 关闭）")
    parser.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR")
    parser.add_argument("--no-dingtalk", action="store_true", help="不发送钉钉，只打印")
    parser.add_argument("--notify-stop", action="store_true", help="退出时发送停止通知")
    parser.add_argument("--duration", type=float, default=0.0, help="运行 N 秒后自动退出（0=一直运行）")
    return parser


def apply_args(args: argparse.Namespace) -> None:
    if args.symbols:
        CONFIG.symbols = tuple(s.strip().upper() for s in args.symbols.split(",") if s.strip())
    if args.intervals:
        CONFIG.intervals = tuple(s.strip() for s in args.intervals.split(",") if s.strip())
    if args.rest_base:
        CONFIG.rest_base = args.rest_base.rstrip("/")
    if args.ws_url:
        CONFIG.ws_url = args.ws_url
    if args.state_file:
        CONFIG.state_file = Path(args.state_file).expanduser().resolve()
    if args.log_file is not None:
        CONFIG.log_file = None if args.log_file.lower() in ("none", "off", "") else Path(args.log_file)
    if args.no_dingtalk:
        CONFIG.ding_enabled = False
    if args.insecure:
        CONFIG.insecure = True
    if args.notify_stop:
        CONFIG.notify_stop = True
    if args.duration:
        CONFIG.duration = args.duration
    CONFIG.log_level = args.log_level

    CONFIG.proxy, CONFIG.proxy_source = detect_proxy(args.proxy)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    command = "run"
    if arguments and arguments[0] in COMMANDS:
        command = arguments.pop(0)

    args = build_parser().parse_args(arguments)
    apply_args(args)
    setup_logging(CONFIG.log_level, CONFIG.log_file)

    if CONFIG.proxy and not CONFIG.proxy.lower().startswith(("http://", "https://")):
        log.warning(
            "检测到非 HTTP 代理（%s），aiohttp 只支持 http(s) 代理；"
            "Clash 请用混合端口，例如 --proxy http://127.0.0.1:7897",
            CONFIG.proxy,
        )

    try:
        if command == "status":
            safe_print(discipline_status())
            return 0

        if command == "record-loss":
            return 0 if record_trade("loss") else 1

        if command == "record-win":
            return 0 if record_trade("win") else 1

        if command == "reset-streak":
            state["loss_streak"] = 0
            save_state()
            safe_print("连续亏损计数已重置")
            safe_print(discipline_status())
            return 0

        if command == "verify":
            return asyncio.run(cmd_verify(args))

        if command == "diag":
            return asyncio.run(cmd_diag(args))

        if command == "selftest":
            return cmd_selftest(args)

        return asyncio.run(monitor())

    except KeyboardInterrupt:
        safe_print("\nTrading Guard 已停止。")
        return 130
    except Exception as exc:  # noqa: BLE001 - 顶层兜底，避免裸 traceback 吓人
        log.critical("程序异常退出：%s", describe_exc(exc))
        log.debug("异常详情", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
