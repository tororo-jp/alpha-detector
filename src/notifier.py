"""
notifier.py
Discordへのスコアリング結果通知モジュール。
"""

import os
import logging
import requests
from dataclasses import dataclass

from scoring_engine import ScoreResult
from xbrl_parser import FinancialSummary

logger = logging.getLogger(__name__)

GRADE_EMOJI = {"S": "🔥", "A": "📈", "B": "📊"}


def notify_result(
    score: ScoreResult,
    summary: FinancialSummary,
    price_data: dict | None,
    margin_data: dict | None,
) -> None:
    """
    スコアリング結果をDiscordに通知する。
    grade が S または A の場合のみ通知（Bはスキップ）。
    """
    if score.grade not in ("S", "A"):
        logger.info(f"[{score.code}] grade={score.grade} → 通知スキップ")
        return

    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not webhook_url:
        logger.error("DISCORD_WEBHOOK_URL が設定されていません")
        return

    message = _format_message(score, summary, price_data, margin_data)

    try:
        res = requests.post(
            webhook_url,
            json={"content": message},
            timeout=15,
        )
        res.raise_for_status()
        logger.info(f"[{score.code}] Discord通知完了 (grade={score.grade})")
    except Exception as e:
        logger.error(f"[{score.code}] Discord通知失敗: {e}")


def notify_error(message: str) -> None:
    """エラー通知（システム異常・PDFパース失敗など）"""
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not webhook_url:
        return
    try:
        requests.post(
            webhook_url,
            json={"content": f"⚠️ **[Alpha-Detector システムエラー]**\n{message}"},
            timeout=15,
        )
    except Exception as e:
        logger.error(f"エラー通知失敗: {e}")


def _format_message(
    score: ScoreResult,
    summary: FinancialSummary,
    price_data: dict | None,
    margin_data: dict | None,
) -> str:
    emoji = GRADE_EMOJI.get(score.grade, "📊")
    q_label = f"{summary.quarter}Q累計"

    # 修正・増配ラベル
    event_labels = []
    if summary.has_upward_revision:
        event_labels.append("上方修正あり ✅")
    if summary.has_dividend_increase:
        event_labels.append("増配あり 💰")
    event_str = " / ".join(event_labels) if event_labels else "修正なし"

    # 利益率
    margin_str = "取得不可"
    if score.margin_now is not None and score.margin_yoy is not None:
        sign = "+" if score.margin_delta >= 0 else ""
        margin_str = (
            f"{score.margin_now:.1f}% "
            f"（前年同期: {score.margin_yoy:.1f}%, "
            f"変化: {sign}{score.margin_delta:.1f}pt）"
        )

    # 株価・需給
    price_str = "取得不可"
    if price_data:
        vs = price_data["vs_index_20d"]
        sign = "+" if vs >= 0 else ""
        price_str = (
            f"終値 {price_data['today_close']:,.0f}円 / "
            f"直近20日対TOPIX {sign}{vs:.1f}%"
        )

    margin_d_str = "取得不可"
    if margin_data:
        margin_d_str = (
            f"信用倍率 {margin_data['ratio']:.1f}倍 "
            f"（買い残 {margin_data['buy']:,.0f}株 / 売り残 {margin_data['sell']:,.0f}株）"
        )

    # 警告
    warning_str = "\n".join(score.warnings) if score.warnings else "なし"

    # スコア内訳
    score_detail = (
        f"進捗スコア: {score.s_progress:.0f}/40 "
        f"| モメンタム: {score.s_momentum:.0f}/30 "
        f"| 修正/増配: {score.s_event:.0f}/30"
    )

    # 保守的据え置き判定
    conservative_note = ""
    if (
        score.progress_delta > 15
        and not summary.has_upward_revision
        and not summary.has_dividend_increase
    ):
        conservative_note = (
            "\n💡 **保守的据え置きに注意**: 進捗率が過去平均を大幅超過しているにもかかわらず"
            "通期修正がありません。発表直後の失望売りリスクがある一方、"
            "下期への期待材料として通期修正余地あり。"
        )

    message = f"""## {emoji} 【{score.total_score}点：{score.grade}評価】 [{summary.code}] {summary.company_name}

### 📈 業績サマリー ({q_label})
- 営業利益進捗率: **{summary.progress_rate:.1f}%**（過去3年平均: {score.avg_progress_3y:.1f}% → **{score.progress_delta:+.1f}%の乖離**）
- 単Q営業利益率: {margin_str}
- イベント: {event_str}

### ⚠️ 需給・織り込みチェック
- 株価: {price_str}
- 信用残: {margin_d_str}

### 🔍 フィルター結果
{warning_str}{conservative_note}

### 📊 スコア内訳（{score.total_score}/100点）
{score_detail}"""

    return message
