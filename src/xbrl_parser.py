"""
xbrl_parser.py
J-Quants API V2のJSONサマリーからXBRL財務データを抽出するモジュール。

【V2変更点】
  - 認証: Authorization Bearer → x-api-key ヘッダー
  - エンドポイント: /v1/documents/{id}/summary → /v2/documents/{id}/summary
  - レスポンスキー: "summary" → "data"[0] の場合あり（要確認）

抽出対象:
  - 売上高（累計）
  - 営業利益（累計）
  - 純利益（累計）
  - 通期予想進捗率
  - 対象四半期
  - 修正フラグ
  - 増配フラグ
"""

import logging
import requests
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# 書類種別コードと意味
TYPE_REVISION    = "160"  # 業績予想修正
TYPE_DIVIDEND    = "170"  # 配当予想修正


@dataclass
class FinancialSummary:
    code: str
    company_name: str
    fiscal_year: int
    quarter: int                     # 1〜4
    cumulative_sales: float          # 累計売上高（万円）
    cumulative_op: float             # 累計営業利益（万円）
    cumulative_net: float            # 累計純利益（万円）
    full_year_op_forecast: float     # 通期営業利益予想（万円）
    progress_rate: float             # 通期予想比進捗率（%）
    has_upward_revision: bool        # 上方修正フラグ
    has_dividend_increase: bool      # 増配フラグ
    document_id: str


def parse_disclosure(doc: dict) -> FinancialSummary | None:
    """
    開示情報から財務サマリーを取得・パースする。（V2: APIキー認証）

    Args:
        doc: fetch_new_disclosures()が返す開示情報dict

    Returns:
        FinancialSummary or None（パース失敗・対象外の場合）
    """
    from tdnet_watcher import _auth_headers
    code       = doc["code"]
    doc_id     = doc["document_id"]
    type_code  = doc["type_code"]

    try:
        summary = _fetch_financial_summary(code, doc_id, _auth_headers())
        if not summary:
            return None

        quarter = _extract_quarter(summary)
        if quarter is None:
            logger.info(f"[{code}] 四半期情報を取得できず → スキップ")
            return None

        # 各財務数値（万円単位に統一: APIは円単位で返すことが多い）
        cum_sales = _to_man_yen(summary.get("NetSales"))
        cum_op    = _to_man_yen(summary.get("OperatingProfit"))
        cum_net   = _to_man_yen(summary.get("NetIncome") or summary.get("Profit"))
        fy_op     = _to_man_yen(summary.get("ForecastOperatingProfit"))

        if cum_op is None or cum_sales is None or cum_net is None:
            logger.warning(f"[{code}] 主要財務数値が欠損 → スキップ")
            return None

        # 進捗率の計算
        if fy_op and fy_op != 0:
            progress_rate = round(cum_op / fy_op * 100, 1)
        else:
            progress_rate = 0.0

        # 修正・増配フラグ
        has_revision = (
            type_code == TYPE_REVISION
            or bool(summary.get("IsRevision"))
            or bool(summary.get("RevisionFlag"))
        )
        has_upward = has_revision and (
            bool(summary.get("IsUpwardRevision"))
            or (
                _to_man_yen(summary.get("ForecastOperatingProfitRevision", 0)) or 0
            ) > 0
        )
        has_div_increase = (
            type_code == TYPE_DIVIDEND
            or bool(summary.get("IsDividendIncrease"))
        )

        fiscal_year = _extract_fiscal_year(summary)

        return FinancialSummary(
            code=code,
            company_name=doc.get("company_name", ""),
            fiscal_year=fiscal_year,
            quarter=quarter,
            cumulative_sales=cum_sales,
            cumulative_op=cum_op,
            cumulative_net=cum_net,
            full_year_op_forecast=fy_op or 0.0,
            progress_rate=progress_rate,
            has_upward_revision=has_upward,
            has_dividend_increase=has_div_increase,
            document_id=doc_id,
        )

    except Exception as e:
        logger.error(f"[{code}] parse_disclosure failed: {e}")
        return None


def _fetch_financial_summary(code: str, doc_id: str, headers: dict) -> dict | None:
    """J-Quants API V2から財務サマリーを取得"""
    url = f"https://api.jquants.com/v2/documents/{doc_id}/summary"
    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        data = res.json()
        # V2レスポンスは {"data": [...]} または {"summary": {...}} の場合がある
        if "data" in data and isinstance(data["data"], list) and data["data"]:
            return data["data"][0]
        return data.get("summary") or data.get("financialStatement") or data
    except Exception as e:
        logger.error(f"[{code}] J-Quants V2 summary fetch failed: {e}")
        return None


def _extract_quarter(summary: dict) -> int | None:
    """四半期番号（1〜4）を取得"""
    # J-Quantsは "TypeOfCurrentPeriod" として "1Q", "2Q" 等を返す
    period = summary.get("TypeOfCurrentPeriod", "")
    mapping = {"1Q": 1, "2Q": 2, "3Q": 3, "4Q": 4, "FY": 4}
    if period in mapping:
        return mapping[period]

    # フォールバック: 月数から推定
    months = summary.get("CumulativeMonths")
    if months:
        return {3: 1, 6: 2, 9: 3, 12: 4}.get(int(months))

    return None


def _extract_fiscal_year(summary: dict) -> int:
    """会計年度（西暦）を取得"""
    fy = summary.get("FiscalYear") or summary.get("FiscalYearEndDate", "")
    if isinstance(fy, int):
        return fy
    if isinstance(fy, str) and len(fy) >= 4:
        try:
            return int(fy[:4])
        except ValueError:
            pass
    from datetime import datetime
    return datetime.now().year


def _to_man_yen(value) -> float | None:
    """円単位の値を万円単位に変換。Noneや無効値はNoneを返す"""
    if value is None:
        return None
    try:
        v = float(value)
        return round(v / 10000, 1)
    except (ValueError, TypeError):
        return None
