"""
scoring_engine.py
Alpha-Detector のスコアリング・フィルタリングロジック。

スコア配点:
  S_progress  (40点): 季節性補正済み進捗スコア
  S_momentum  (30点): モメンタム加速スコア（単Q利益率改善）
  S_event     (30点): 上方修正・増配フラグ
  合計 100点満点 / 80点以上 → S評価（即時通知）

フィルター（スコアによらず警告）:
  - 期待値織り込み: 対TOPIX乖離率 > +15%
  - 利益の質:      純利益÷営業利益 > 1.5（かつ営業利益 > 0）
                   営業赤字・ゼロ時は別途警告
  - 需給悪化:      信用倍率 > 10倍
  - 過去履歴不足:  過去3年のデータがない → 除外（スコア算出せず）
"""

import logging
from dataclasses import dataclass, field

from history_db import QuarterlyResult, get_history
from xbrl_parser import FinancialSummary

logger = logging.getLogger(__name__)

# ── スコア閾値 ──────────────────────────────────
GRADE_S_THRESHOLD = 80
GRADE_A_THRESHOLD = 60

# ── スコアリングパラメータ ──────────────────────
PROGRESS_FULL_MARK_DELTA = 10.0  # 進捗率がこの%ポイント超で満点（40点）
MOMENTUM_FULL_MARK_DELTA =  2.0  # 利益率がこのptポイント超で満点（30点）

# ── フィルター閾値 ──────────────────────────────
FILTER_VS_INDEX_20D      = 15.0  # 対TOPIX乖離率（%）
FILTER_PROFIT_QUALITY    =  1.5  # 純利益÷営業利益
FILTER_SHINYO_BAIRITSU   = 10.0  # 信用倍率


@dataclass
class ScoreResult:
    code: str
    company_name: str
    total_score: int
    grade: str                         # S / A / B / SKIP
    s_progress: float
    s_momentum: float
    s_event: float
    avg_progress_3y: float             # 過去3年平均進捗率
    progress_delta: float              # 今回 − 過去3年平均
    margin_now: float | None           # 当Q単Q営業利益率
    margin_yoy: float | None           # 前年同Q単Q営業利益率
    margin_delta: float | None         # 利益率改善幅
    warnings: list[str] = field(default_factory=list)
    skip_reason: str | None = None     # SKIP時の理由


def run_scoring(
    summary: FinancialSummary,
    price_data: dict | None,
    margin_data: dict | None,
) -> ScoreResult:
    """
    メインスコアリング関数。

    Args:
        summary   : xbrl_parser.FinancialSummary
        price_data: price_analyzer.get_price_data() の戻り値
        margin_data: history_db.get_margin_data() の戻り値

    Returns:
        ScoreResult（grade=="SKIP"の場合は通知対象外）
    """
    code = summary.code
    name = summary.company_name

    # ── 過去3年履歴チェック（必須） ───────────────
    history = get_history(code, summary.quarter)
    if not history:
        return ScoreResult(
            code=code, company_name=name,
            total_score=0, grade="SKIP",
            s_progress=0, s_momentum=0, s_event=0,
            avg_progress_3y=0, progress_delta=0,
            margin_now=None, margin_yoy=None, margin_delta=None,
            skip_reason="過去3年の履歴データ不足",
        )

    # ── ① 季節性補正済み進捗スコア（40点） ────────
    avg_progress = sum(h.progress_rate for h in history) / len(history)
    delta_progress = summary.progress_rate - avg_progress
    s_progress = _clamp(40.0 * (delta_progress / PROGRESS_FULL_MARK_DELTA), 0, 40)

    # ── ② モメンタム加速スコア（30点） ────────────
    single_q = _calc_single_quarter(summary, history)
    margin_now   = single_q.get("margin_now")
    margin_yoy   = single_q.get("margin_yoy")
    margin_delta = single_q.get("margin_delta")

    if margin_delta is not None:
        s_momentum = _clamp(30.0 * (margin_delta / MOMENTUM_FULL_MARK_DELTA), 0, 30)
    else:
        s_momentum = 0.0

    # ── ③ 上方修正・増配フラグ（30点） ────────────
    s_event = 30.0 if (summary.has_upward_revision or summary.has_dividend_increase) else 0.0

    # ── 合計スコア ─────────────────────────────────
    total = int(s_progress + s_momentum + s_event)
    grade = (
        "S" if total >= GRADE_S_THRESHOLD else
        "A" if total >= GRADE_A_THRESHOLD else
        "B"
    )

    # ── フィルター ─────────────────────────────────
    warnings = []

    # 期待値織り込みチェック
    if price_data:
        vs_index = price_data.get("vs_index_20d", 0)
        if vs_index > FILTER_VS_INDEX_20D:
            warnings.append(
                f"⚠️ 期待値織り込み済み（直近20日対TOPIX+{vs_index:.1f}%）"
            )

    # 利益の質チェック（ゼロ除算ガード付き）
    profit_quality_warning = _check_profit_quality(
        summary.cumulative_op, summary.cumulative_net
    )
    if profit_quality_warning:
        warnings.append(profit_quality_warning)

    # 需給チェック
    if margin_data:
        ratio = margin_data.get("ratio", 0)
        if ratio > FILTER_SHINYO_BAIRITSU:
            warnings.append(f"⚠️ 信用倍率{ratio:.1f}倍（需給悪化）")

    return ScoreResult(
        code=code, company_name=name,
        total_score=total, grade=grade,
        s_progress=round(s_progress, 1),
        s_momentum=round(s_momentum, 1),
        s_event=s_event,
        avg_progress_3y=round(avg_progress, 1),
        progress_delta=round(delta_progress, 1),
        margin_now=margin_now,
        margin_yoy=margin_yoy,
        margin_delta=margin_delta,
        warnings=warnings,
    )


# ── 単Q計算 ──────────────────────────────────────

def _calc_single_quarter(
    summary: FinancialSummary,
    history: list[QuarterlyResult],
) -> dict:
    """
    単Q営業利益率を計算する。
    累計から逆算せず、「累計 − 前Q累計」で計算。
    前Q・前年データが欠損する場合は None を返す（誤値を入れない）。

    Returns:
        {
          "margin_now"  : float | None,  # 当期単Q営業利益率(%)
          "margin_yoy"  : float | None,  # 前年同期単Q営業利益率(%)
          "margin_delta": float | None,  # 改善幅
        }
    """
    q = summary.quarter

    # 1Qは累計=単Q（逆算不要）
    if q == 1:
        single_op_now    = summary.cumulative_op
        single_sales_now = summary.cumulative_sales
    else:
        # 前Q（当期）の累計データがDBにあるか確認
        prev_q_data = _find_prev_q_in_history(history, summary.fiscal_year, q - 1)
        if prev_q_data is None:
            logger.info(
                f"[{summary.code}] {summary.fiscal_year}年{q-1}Q累計データ未取得 "
                f"→ 単Q計算スキップ"
            )
            return {"margin_now": None, "margin_yoy": None, "margin_delta": None}

        single_op_now    = summary.cumulative_op    - prev_q_data.cumulative_op
        single_sales_now = summary.cumulative_sales - prev_q_data.cumulative_sales

    # 前年同期履歴から単Qを計算
    yoy_current = _find_in_history(history, summary.fiscal_year - 1, q)
    if yoy_current is None:
        logger.info(f"[{summary.code}] 前年{q}Q履歴なし → 単Q計算スキップ")
        return {"margin_now": None, "margin_yoy": None, "margin_delta": None}

    if q == 1:
        single_op_yoy    = yoy_current.cumulative_op
        single_sales_yoy = yoy_current.cumulative_sales
    else:
        yoy_prev_q = _find_in_history(history, summary.fiscal_year - 1, q - 1)
        if yoy_prev_q is None:
            return {"margin_now": None, "margin_yoy": None, "margin_delta": None}
        single_op_yoy    = yoy_current.cumulative_op    - yoy_prev_q.cumulative_op
        single_sales_yoy = yoy_current.cumulative_sales - yoy_prev_q.cumulative_sales

    margin_now = _safe_margin(single_op_now, single_sales_now)
    margin_yoy = _safe_margin(single_op_yoy, single_sales_yoy)

    if margin_now is None or margin_yoy is None:
        return {"margin_now": margin_now, "margin_yoy": margin_yoy, "margin_delta": None}

    return {
        "margin_now"  : margin_now,
        "margin_yoy"  : margin_yoy,
        "margin_delta": round(margin_now - margin_yoy, 2),
    }


def _find_in_history(
    history: list[QuarterlyResult], fiscal_year: int, quarter: int
) -> QuarterlyResult | None:
    for h in history:
        if h.fiscal_year == fiscal_year and h.quarter == quarter:
            return h
    return None


def _find_prev_q_in_history(
    history: list[QuarterlyResult], fiscal_year: int, quarter: int
) -> QuarterlyResult | None:
    """
    前Q累計データをhistoryから探す。
    historyには「同Qの過去3年分」しか入っていないため、
    前Q取得には別途DBクエリが必要なことに注意。
    ここでは保守的にNoneを返す（main.pyで前Q取得を別途行う設計）。
    """
    # history は「同Q・異なる年度」のリストなので前Qは含まれない
    # main.pyでget_history(code, q-1)を呼び出し、この関数に渡すこと
    return _find_in_history(history, fiscal_year, quarter)


# ── ユーティリティ ───────────────────────────────

def _safe_margin(op: float, sales: float) -> float | None:
    """ゼロ除算ガード付き営業利益率計算"""
    if sales is None or sales == 0:
        return None
    return round(op / sales * 100, 2)


def _check_profit_quality(op: float, net: float) -> str | None:
    """
    利益の質フィルター（ゼロ除算ガード付き）。

    ケース分類:
      - 営業利益 > 0 かつ 純利益/営業利益 > 1.5 → 特損・資産売却の可能性
      - 営業利益 <= 0 かつ 純利益 > 0 → 営業赤字だが最終黒字（特益の可能性）
      - 営業利益 < 0 かつ 純利益 < 0 → 営業赤字（純赤字）
    """
    if op > 0:
        ratio = net / op
        if ratio > FILTER_PROFIT_QUALITY:
            return f"⚠️ 利益の質に懸念（純利益/営業利益={ratio:.2f}：特別利益の可能性）"
    elif op == 0:
        return "⚠️ 営業利益ゼロ（収益性に注意）"
    else:  # op < 0
        if net > 0:
            return f"⚠️ 営業赤字（{op:.0f}万円）だが最終黒字：特別利益による可能性"
        else:
            return f"⚠️ 営業赤字（{op:.0f}万円）"
    return None


def _clamp(value: float, min_val: float, max_val: float) -> float:
    return max(min_val, min(max_val, value))
