"""
price_analyzer.py
yfinanceを使って株価・出来高データを取得するモジュール。

設計方針:
  - 銘柄間のsleepで人間らしいアクセスに偽装（Botブロック対策）
  - 429エラー時は30秒waitしてリトライ（最大3回）
  - TOPIXはプロセス内キャッシュで1回だけ取得
  - 取得失敗銘柄はスキップ（Noneを返す）してシステム全体は継続
"""

import time
import random
import logging
from datetime import datetime

import yfinance as yf
import pandas as pd

logger = logging.getLogger(__name__)

# ── sleepの設定 ──────────────────────────────
SLEEP_BASE        = 2.0   # 銘柄間の基本sleep（秒）
SLEEP_JITTER_MAX  = 1.0   # ランダム揺らぎの最大値（秒）
SLEEP_TOPIX       = 1.0   # TOPIX取得後のsleep（秒）
SLEEP_RATE_LIMIT  = 30.0  # 429エラー時のwait（秒）
MAX_RETRY         = 3     # 最大リトライ回数
LOOKBACK_DAYS     = 35    # 取得日数（30営業日+祝日バッファ）

# ── グローバルキャッシュ ──────────────────────
_topix_close_20d: list[float] | None = None
_topix_fetched_date: str | None = None


def _sleep(base: float = SLEEP_BASE) -> None:
    """ランダム揺らぎ付きsleep"""
    duration = base + random.uniform(0, SLEEP_JITTER_MAX)
    time.sleep(duration)


def _get_topix_return_20d() -> float:
    """
    TOPIXの直近20営業日騰落率を返す。
    プロセス内でキャッシュし、1日1回のみ取得。
    """
    global _topix_close_20d, _topix_fetched_date

    today = datetime.now().strftime("%Y%m%d")
    if _topix_fetched_date == today and _topix_close_20d:
        return _calc_return(list(_topix_close_20d))

    try:
        hist = yf.Ticker("^TPX").history(period=f"{LOOKBACK_DAYS}d")
        closes = hist["Close"].tail(20).tolist()
        if len(closes) >= 2:
            _topix_close_20d = closes
            _topix_fetched_date = today
            _sleep(SLEEP_TOPIX)
            return _calc_return(closes)
    except Exception as e:
        logger.warning(f"TOPIX取得失敗: {e}")

    return 0.0


def _calc_return(closes: list[float]) -> float:
    """リスト先頭から末尾への騰落率（%）を計算"""
    if len(closes) < 2 or closes[0] == 0:
        return 0.0
    return round((closes[-1] - closes[0]) / closes[0] * 100, 2)


def get_price_data(code: str, retry: int = 0) -> dict | None:
    """
    1銘柄の株価・出来高データを取得する。

    Args:
        code: 証券コード（4桁）
        retry: 内部リトライカウント（外部から指定不要）

    Returns:
        {
          "today_close"   : float,   # 当日終値
          "today_volume"  : int,     # 当日出来高
          "avg_volume_5d" : int,     # 5日平均出来高
          "vs_index_20d"  : float,   # 対TOPIX乖離率（%）
        }
        or None（取得失敗時）

    Note:
        この関数の呼び出し後にsleep済み。
        連続で呼び出しても安全。
    """
    ticker_symbol = f"{code}.T"

    try:
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(period=f"{LOOKBACK_DAYS}d")

        if hist.empty or len(hist) < 5:
            logger.warning(f"[{code}] 株価データ不足（{len(hist)}行）")
            _sleep()
            return None

        close_20d  = hist["Close"].tail(20)
        volume_20d = hist["Volume"].tail(20)

        today_close  = float(close_20d.iloc[-1])
        today_volume = int(volume_20d.iloc[-1])
        avg_vol_5d   = int(volume_20d.tail(5).mean())

        # 対指数乖離率 = 銘柄20日騰落率 − TOPIX20日騰落率
        stock_return  = _calc_return(close_20d.tolist())
        topix_return  = _get_topix_return_20d()
        vs_index      = round(stock_return - topix_return, 2)

        _sleep()  # ← 次の銘柄へのsleep（必ず実行）

        return {
            "today_close"  : round(today_close, 1),
            "today_volume" : today_volume,
            "avg_volume_5d": avg_vol_5d,
            "vs_index_20d" : vs_index,
        }

    except Exception as e:
        err_str = str(e)

        # 429 Rate limit → waitしてリトライ
        if ("429" in err_str or "Too Many Requests" in err_str) and retry < MAX_RETRY:
            logger.warning(
                f"[{code}] Rate limit (429). {SLEEP_RATE_LIMIT}秒待機... "
                f"(retry {retry + 1}/{MAX_RETRY})"
            )
            time.sleep(SLEEP_RATE_LIMIT)
            return get_price_data(code, retry + 1)

        logger.error(f"[{code}] 株価取得失敗 (retry={retry}): {e}")
        _sleep()  # 失敗時もsleepを挟む
        return None
