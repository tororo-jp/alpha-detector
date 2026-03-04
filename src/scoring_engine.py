"""
scoring_engine.py
=================
Alpha-Detector スコアリング・フィルタリングロジック。

配点:
  S_progress (40点): 季節性補正済み進捗スコア
  S_momentum (30点): 単Q営業利益率の前年同期比改善
  S_event    (30点): 上方修正 or 増配フラグ
  合計 100点 / 80点以上→S評価 / 60点以上→A評価

フィルター（スコアによらず警告を付与）:
  - 期待値織り込み: 対TOPIX乖離率 > +15%
  - 利益の質:      純利益÷営業利益 > 1.5（営業赤字も警告）
  - 需給悪化:      信用倍率 > 10倍
"""

import logging
from dataclasses import dataclass, field

from history_db import QuarterlyResult
from xbrl_parser import FinancialSummary

logger = logging.getLogger(__name__)

GRADE_S = 80
GRADE_A = 60

PROGRESS_FULL_DELTA  = 10.0   # 進捗率乖離がこの%pt超で40点満点
MOMENTUM_FULL_DELTA  =  2.0   # 利益率改善がこのpt超で30点満点
FILTER_VS_INDEX      = 15.0
FILTER_PROFIT_QUALITY = 1.5
FILTER_SHINYO        = 10.0


@dataclass
class ScoreResult:
    code            : str
    company_name    : str
    total_score     : int
    grade           : str               # S / A / B / SKIP
    s_progress      : float
    s_momentum      : float
    s_event         : float
    avg_progress_3y : float
    progress_delta  : float
    margin_now      : float | None
    margin_yoy      : float | None
    margin_delta    : float | None
    warnings        : list[str] = field(default_factory=list)
    skip_reason     : str | None = None


def run_scoring(
    summary     : FinancialSummary,
    history     : list[QuarterlyResult],   # 同Q・過去3年分
    price_data  : dict | None,
    margin_data : dict | None,
) -> ScoreResult:
    code = summary.code
    name = summary.company_name

    # ── ① 季節性補正済み進捗スコア（40点） ───────────────────────────
    avg_progress   = sum(h.progress_rate for h in history) / len(history)
    delta_progress = summary.progress_rate - avg_progress
    s_progress     = _clamp(40.0 * delta_progress / PROGRESS_FULL_DELTA, 0, 40)

    # ── ② 単Q営業利益率モメンタム（30点） ────────────────────────────
    sq = _calc_single_quarter(summary, history)
    margin_now   = sq["margin_now"]
    margin_yoy   = sq["margin_yoy"]
    margin_delta = sq["margin_delta"]
    s_momentum   = _clamp(30.0 * margin_delta / MOMENTUM_FULL_DELTA, 0, 30) \
                   if margin_delta is not None else 0.0

    # ── ③ 上方修正・増配フラグ（30点） ───────────────────────────────
    s_event = 30.0 if (summary.has_upward_revision or summary.has_dividend_increase) else 0.0

    total = int(s_progress + s_momentum + s_event)
    grade = "S" if total >= GRADE_S else "A" if total >= GRADE_A else "B"

    # ── フィルター ────────────────────────────────────────────────────
    warnings = []
    if price_data:
        vs = price_data.get("vs_index_20d", 0)
        if vs > FILTER_VS_INDEX:
            warnings.append(f"⚠️ 期待値織り込み済み（直近20日対TOPIX+{vs:.1f}%）")

    pq_warn = _check_profit_quality(summary.cumulative_op, summary.cumulative_net)
    if pq_warn:
        warnings.append(pq_warn)

    if margin_data:
        ratio = margin_data.get("ratio", 0)
        if ratio > FILTER_SHINYO:
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


# ── 単Q計算 ──────────────────────────────────────────────────────────

def _calc_single_quarter(summary: FinancialSummary, history: list[QuarterlyResult]) -> dict:
    """
    単Q営業利益率を「今期累計 − 前Q累計」で計算する。
    1Qは累計=単Q（逆算不要）。
    前Q・前年データが欠損する場合はNoneを返す。
    """
    q  = summary.quarter
    fy = summary.fiscal_year

    # ── 当Q単Q値 ──────────────────────────────────────────────────────
    if q == 1:
        sq_op_now    = summary.cumulative_op
        sq_sales_now = summary.cumulative_sales
    else:
        # 当期の前Q累計をhistoryから探す
        prev_q = _find(history, fy, q - 1)
        if prev_q is None:
            logger.info(f"[{summary.code}] 当期{q-1}Q累計データなし → 単Q計算スキップ")
            return {"margin_now": None, "margin_yoy": None, "margin_delta": None}
        sq_op_now    = summary.cumulative_op    - prev_q.cumulative_op
        sq_sales_now = summary.cumulative_sales - prev_q.cumulative_sales

    # ── 前年同Q単Q値 ──────────────────────────────────────────────────
    yoy_curr = _find(history, fy - 1, q)
    if yoy_curr is None:
        logger.info(f"[{summary.code}] 前年{q}Q履歴なし → 単Q計算スキップ")
        return {"margin_now": None, "margin_yoy": None, "margin_delta": None}

    if q == 1:
        sq_op_yoy    = yoy_curr.cumulative_op
        sq_sales_yoy = yoy_curr.cumulative_sales
    else:
        yoy_prev = _find(history, fy - 1, q - 1)
        if yoy_prev is None:
            return {"margin_now": None, "margin_yoy": None, "margin_delta": None}
        sq_op_yoy    = yoy_curr.cumulative_op    - yoy_prev.cumulative_op
        sq_sales_yoy = yoy_curr.cumulative_sales - yoy_prev.cumulative_sales

    margin_now = _margin(sq_op_now,  sq_sales_now)
    margin_yoy = _margin(sq_op_yoy,  sq_sales_yoy)

    return {
        "margin_now"  : margin_now,
        "margin_yoy"  : margin_yoy,
        "margin_delta": round(margin_now - margin_yoy, 2)
                        if margin_now is not None and margin_yoy is not None else None,
    }


def _find(history: list[QuarterlyResult], fy: int, q: int) -> QuarterlyResult | None:
    for h in history:
        if h.fiscal_year == fy and h.quarter == q:
            return h
    return None


def _margin(op: float, sales: float) -> float | None:
    if not sales:
        return None
    return round(op / sales * 100, 2)


def _check_profit_quality(op: float, net: float) -> str | None:
    if op > 0:
        ratio = net / op
        if ratio > FILTER_PROFIT_QUALITY:
            return f"⚠️ 利益の質に懸念（純利益/営業利益={ratio:.2f}：特別利益の可能性）"
    elif op == 0:
        return "⚠️ 営業利益ゼロ（収益性に注意）"
    else:
        return f"⚠️ 営業赤字（{op:.0f}万円）" + \
               ("だが最終黒字：特別利益の可能性" if net > 0 else "")
    return None


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))
