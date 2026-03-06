"""
notifier.py
===========
Discord Webhook へのスコアリング結果通知モジュール。
"""

import logging
import os
import requests

from scoring_engine import ScoreResult
from xbrl_parser import FinancialSummary

logger = logging.getLogger(__name__)
GRADE_EMOJI = {"S": "🔥", "A": "📈", "B": "📊"}


def notify_result(
    summary     : FinancialSummary,
    score       : ScoreResult,
    price_data  : dict | None,
    margin_data : dict | None,
) -> None:
    if score.grade not in ("S", "A"):
        return
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not webhook_url:
        logger.error("DISCORD_WEBHOOK_URL が設定されていません")
        return
    msg = _format(summary, score, price_data, margin_data)
    try:
        requests.post(webhook_url, json={"content": msg}, timeout=15).raise_for_status()
        logger.info(f"[{score.code}] Discord通知完了 ({score.grade}評価)")
    except Exception as e:
        logger.error(f"[{score.code}] Discord通知失敗: {e}")


def notify_error(message: str) -> None:
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not webhook_url:
        return
    try:
        requests.post(
            webhook_url,
            json={"content": f"⚠️ **[Alpha-Detector エラー]**\n{message}"},
            timeout=15,
        )
    except Exception:
        pass


def _format(
    summary     : FinancialSummary,
    score       : ScoreResult,
    price_data  : dict | None,
    margin_data : dict | None,
) -> str:
    emoji   = GRADE_EMOJI.get(score.grade, "📊")
    q_label = f"{summary.quarter}Q累計"

    event_parts = []
    if summary.has_upward_revision:
        event_parts.append("上方修正 ✅")
    if summary.has_dividend_increase:
        event_parts.append("増配 💰")
    event_str = " / ".join(event_parts) if event_parts else "修正なし"

    if score.margin_now is not None and score.margin_yoy is not None:
        sign = "+" if score.margin_delta >= 0 else ""
        margin_str = (
            f"{score.margin_now:.1f}%"
            f"（前年同期:{score.margin_yoy:.1f}%  変化:{sign}{score.margin_delta:.1f}pt）"
        )
    else:
        margin_str = "算出不可（前Q/前年データ不足）"

    if price_data:
        vs   = price_data["vs_index_20d"]
        sign = "+" if vs >= 0 else ""
        price_str = f"{price_data['today_close']:,.0f}円  直近20日対TOPIX:{sign}{vs:.1f}%"
    else:
        price_str = "取得不可"

    if margin_data:
        shinyo_str = (
            f"信用倍率{margin_data['ratio']:.1f}倍"
            f"（買:{margin_data['buy']:,.0f}株 / 売:{margin_data['sell']:,.0f}株）"
        )
    else:
        shinyo_str = "取得不可"

    warning_str = "\n".join(score.warnings) if score.warnings else "なし"

    conservative = ""
    if score.progress_delta > 15 and not summary.has_upward_revision:
        conservative = (
            "\n💡 **保守的据え置きに注意**: 進捗率が大幅超過なのに通期修正なし。"
            "失望売りリスクと通期修正余地が共存。"
        )

    return (
        f"## {emoji}【{score.total_score}点:{score.grade}評価】"
        f"[{summary.code}] {summary.company_name}\n\n"
        f"### 📈 業績サマリー（{q_label}）\n"
        f"- 進捗率: **{summary.progress_rate:.1f}%**"
        f"（過去3年平均:{score.avg_progress_3y:.1f}%  乖離:**{score.progress_delta:+.1f}%**）\n"
        f"- 単Q営業利益率: {margin_str}\n"
        f"- イベント: {event_str}\n\n"
        f"### ⚠️ 需給・織り込みチェック\n"
        f"- 株価: {price_str}\n"
        f"- 信用残: {shinyo_str}\n\n"
        f"### 🔍 フィルター\n"
        f"{warning_str}{conservative}\n\n"
        f"### 📊 スコア内訳（{score.total_score}/100点）\n"
        f"進捗:{score.s_progress:.0f}/40  "
        f"モメンタム:{score.s_momentum:.0f}/30  "
        f"修正/増配:{score.s_event:.0f}/30"
    )
