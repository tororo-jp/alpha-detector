"""
xbrl_parser.py
==============
TDnet XBRLのZIPをダウンロード・展開し、
Summaryフォルダの ixbrl.htm から財務数値を抽出するモジュール。

【処理フロー】
  XBRL ZIP URL
    ↓ ダウンロード（requests）
    ↓ メモリ上でZIP展開（zipfile）
  Summary/*ixbrl*.htm
    ↓ BeautifulSoupで ix:nonfraction / ix:nonnumeric タグを解析
  FinancialSummary（dataclass）

【対応会計基準】
  JP GAAP / IFRS / US GAAP（タグ名優先順で最初に見つかった値を使用）

【金額の単位変換】
  scale属性（百万円単位など）→ 円単位 → 万円単位に統一
"""

import io
import logging
import re
import time
import zipfile
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)
SLEEP_SEC = 1.5


# ── FinancialSummary（全モジュール共通のデータクラス）──────────────────

@dataclass
class FinancialSummary:
    code              : str         = ""
    company_name      : str         = ""
    fiscal_year_end   : str         = ""    # "YYYY-MM-DD"
    fiscal_year       : int         = 0     # 会計年度（fiscal_year_end の年）
    quarter           : int         = 0     # 1/2/3/4
    # 累計値（万円）
    cumulative_sales  : float       = 0.0
    cumulative_op     : float       = 0.0   # 営業利益
    cumulative_net    : float       = 0.0   # 純利益
    # 通期予想営業利益（万円）
    forecast_op       : float | None = None
    # 今回の進捗率（%）: cumulative_op / forecast_op * 100
    progress_rate     : float       = 0.0
    # 修正・配当フラグ
    has_upward_revision    : bool   = False
    has_dividend_increase  : bool   = False
    prev_dividend     : float | None = None
    curr_dividend     : float | None = None


# ── XBRL タグ定義 ─────────────────────────────────────────────────────

SALES_TAGS = [
    "NetSales",
    "NetSalesIFRS",
    "RevenueIFRS",
    "SalesIFRS",
    "NetSalesUS",
    "OperatingRevenues",
    "OperatingRevenuesIFRS",
]
OP_TAGS = [
    "OperatingIncome",
    "OperatingProfit",
    "OperatingProfitLossIFRS",
    "OperatingIncomeUS",
    "OrdinaryIncome",
]
NET_TAGS = [
    "NetIncome",
    "ProfitLossIFRS",
    "NetIncomeUS",
    "ProfitAttributableToOwnersOfParentIFRS",
    "ProfitLoss",
]
FORECAST_OP_TAGS = [
    "ForecastOperatingIncome",
    "ForecastOperatingProfit",
    "ForecastOperatingProfitLossIFRS",
]

PERIOD_MAP = {
    "q1": 1, "q2": 2, "q3": 3, "q4": 4,
    "1q": 1, "2q": 2, "3q": 3, "4q": 4,
    "fy": 4, "annual": 4,
}

CURRENT_CTX = re.compile(
    r"(CurrentYearDuration|CurrentAccumulatedQ[1-3]Duration)",
    re.IGNORECASE,
)
FORECAST_CTX = re.compile(r"Forecast", re.IGNORECASE)


# ── ユーティリティ ────────────────────────────────────────────────────

def _to_float(text: str) -> float | None:
    if not text:
        return None
    text = text.strip().replace(",", "").replace("\xa0", "").replace(" ", "")
    negative = text.startswith("△") or text.startswith("-")
    text = text.lstrip("△").lstrip("-").strip()
    try:
        return -float(text) if negative else float(text)
    except ValueError:
        return None


def _to_man_yen(val: float, tag: Tag) -> float:
    """scale属性を適用して円→万円変換"""
    try:
        scale = int(tag.get("scale") or tag.get("Scale") or 0)
    except (ValueError, TypeError):
        scale = 0
    return round(val * (10 ** scale) / 10_000, 1)


def _find_value(soup: BeautifulSoup, short_names: list[str], ctx_pat: re.Pattern) -> float | None:
    for name in short_names:
        elems = soup.find_all(
            "ix:nonfraction",
            attrs={"name": re.compile(name, re.IGNORECASE)},
        )
        for elem in elems:
            ctx = elem.get("contextref") or elem.get("contextRef") or ""
            if ctx_pat.search(ctx):
                raw = _to_float(elem.get_text())
                if raw is not None:
                    return _to_man_yen(raw, elem)
    return None


def _detect_quarter(soup: BeautifulSoup) -> int:
    for tag in soup.find_all("ix:nonnumeric"):
        name = (tag.get("name") or "").lower()
        if "typeofcurrentperiod" in name:
            val = tag.get_text(strip=True).lower()
            for k, v in PERIOD_MAP.items():
                if k in val:
                    return v
    for elem in soup.find_all("ix:nonfraction"):
        ctx = (elem.get("contextref") or elem.get("contextRef") or "").lower()
        if "accumulatedq3" in ctx: return 3
        if "accumulatedq2" in ctx: return 2
        if "accumulatedq1" in ctx: return 1
        if "currentyear"   in ctx: return 4
    return 0


def _detect_fiscal_year_end(soup: BeautifulSoup) -> str:
    for tag in soup.find_all("ix:nonnumeric"):
        name = (tag.get("name") or "").lower()
        if "fiscalyearend" in name or "currentfiscalyearenddate" in name:
            val = tag.get_text(strip=True).replace("/", "-")
            if re.match(r"\d{4}-\d{2}-\d{2}", val):
                return val
    return ""


def _detect_flags(soup: BeautifulSoup, title: str) -> tuple[bool, bool, float | None, float | None]:
    """(has_upward_revision, has_dividend_increase, prev_div, curr_div)"""
    is_rev    = "業績予想の修正" in title
    is_div_up = False
    prev_div  = None
    curr_div  = None
    if "配当予想の修正" in title:
        for tag in soup.find_all("ix:nonfraction"):
            name = (tag.get("name") or "").lower()
            if "dividendpershare" in name:
                val = _to_float(tag.get_text())
                ctx = (tag.get("contextref") or "").lower()
                if "before" in ctx or "prior" in ctx:
                    prev_div = val
                elif "after" in ctx or "revised" in ctx:
                    curr_div = val
        if prev_div is not None and curr_div is not None:
            is_div_up = curr_div > prev_div
    return is_rev, is_div_up, prev_div, curr_div


# ── ZIPダウンロード・パース ───────────────────────────────────────────

def _download_and_parse(zip_url: str, retry: int = 0) -> BeautifulSoup | None:
    try:
        res = requests.get(zip_url, timeout=20)
        if res.status_code == 429 and retry < 3:
            logger.warning("429 Rate limit. 30秒待機...")
            time.sleep(30)
            return _download_and_parse(zip_url, retry + 1)
        res.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"ZIP取得失敗 {zip_url}: {e}")
        return None
    try:
        with zipfile.ZipFile(io.BytesIO(res.content)) as zf:
            candidates = [
                n for n in zf.namelist()
                if "summary" in n.lower() and n.lower().endswith((".htm", ".html"))
            ]
            if not candidates:
                candidates = [
                    n for n in zf.namelist()
                    if "ixbrl" in n.lower() and n.lower().endswith((".htm", ".html"))
                ]
            if not candidates:
                logger.warning(f"ixbrl.htm が見つかりません: {zip_url}")
                return None
            content = zf.read(candidates[0]).decode("utf-8", errors="replace")
    except (zipfile.BadZipFile, KeyError) as e:
        logger.warning(f"ZIP展開失敗: {e}")
        return None
    return BeautifulSoup(content, "html.parser")


# ── メイン関数 ────────────────────────────────────────────────────────

def parse_disclosure(doc: dict) -> "FinancialSummary | None":
    """
    開示情報dict → FinancialSummary。
    失敗時は None を返す。

    Args:
        doc: {
          "document_id": str,
          "code": str,
          "company_name": str,
          "title": str,
          "xbrl_zip_url": str,
        }
    """
    zip_url = doc.get("xbrl_zip_url", "")
    title   = doc.get("title", "")
    code    = doc.get("code", "")

    if not zip_url:
        return None

    logger.info(f"[{code}] XBRL取得中: {zip_url}")
    soup = _download_and_parse(zip_url)
    if soup is None:
        return None

    time.sleep(SLEEP_SEC)

    cum_sales  = _find_value(soup, SALES_TAGS,       CURRENT_CTX)
    cum_op     = _find_value(soup, OP_TAGS,           CURRENT_CTX)
    cum_net    = _find_value(soup, NET_TAGS,           CURRENT_CTX)
    forecast   = _find_value(soup, FORECAST_OP_TAGS,  FORECAST_CTX)

    if cum_op is None or cum_sales is None:
        logger.warning(f"[{code}] 営業利益 or 売上高が取得できませんでした")
        return None

    quarter        = _detect_quarter(soup)
    fy_end         = _detect_fiscal_year_end(soup)
    fiscal_year    = int(fy_end[:4]) if fy_end else 0

    if quarter == 0:
        logger.warning(f"[{code}] 四半期区分が判定できませんでした")
        return None

    progress = round(cum_op / forecast * 100, 1) if forecast else 0.0
    is_rev, is_div_up, prev_div, curr_div = _detect_flags(soup, title)

    return FinancialSummary(
        code                  = code,
        company_name          = doc.get("company_name", ""),
        fiscal_year_end       = fy_end,
        fiscal_year           = fiscal_year,
        quarter               = quarter,
        cumulative_sales      = cum_sales,
        cumulative_op         = cum_op,
        cumulative_net        = cum_net if cum_net is not None else 0.0,
        forecast_op           = forecast,
        progress_rate         = progress,
        has_upward_revision   = is_rev,
        has_dividend_increase = is_div_up,
        prev_dividend         = prev_div,
        curr_dividend         = curr_div,
    )
