"""
history_db.py
=============
Google Sheetsを財務履歴DBとして使用するモジュール。

シート構成:
  - "history"  : 過去の四半期財務データ
  - "margin"   : 最新週次の信用残データ
  - "processed": 処理済み開示IDの管理

historyシートのカラム:
  code | fiscal_year | quarter | cumulative_sales | cumulative_op
  | cumulative_net | progress_rate | updated_at
"""

import json
import os
import logging
from dataclasses import dataclass
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HISTORY_SHEET   = "history"
MARGIN_SHEET    = "margin"
PROCESSED_SHEET = "processed"

MIN_HISTORY_COUNT = 3


@dataclass
class QuarterlyResult:
    code             : str
    fiscal_year      : int
    quarter          : int
    cumulative_sales : float
    cumulative_op    : float
    cumulative_net   : float
    progress_rate    : float


def _get_client() -> gspread.Client:
    creds_dict = json.loads(os.environ["GOOGLE_SHEETS_CREDS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def _get_sheet(sheet_name: str) -> gspread.Worksheet:
    sheet_id = os.environ["GOOGLE_SHEET_ID"]
    return _get_client().open_by_key(sheet_id).worksheet(sheet_name)


# ── 財務履歴の取得 ─────────────────────────────────────────────────

def get_history(code: str, quarter: int) -> list[QuarterlyResult]:
    """
    指定銘柄・四半期の過去履歴を返す。
    MIN_HISTORY_COUNT未満の場合は空リストを返す（除外対象）。
    """
    try:
        ws      = _get_sheet(HISTORY_SHEET)
        records = ws.get_all_records()
    except Exception as e:
        logger.error(f"[{code}] history DB fetch failed: {e}")
        return []

    matched = [
        r for r in records
        if str(r.get("code")) == str(code)
        and int(r.get("quarter", 0)) == quarter
    ]
    matched.sort(key=lambda r: r.get("fiscal_year", 0))
    matched = matched[-3:]

    if len(matched) < MIN_HISTORY_COUNT:
        return []

    results = []
    for r in matched:
        try:
            results.append(QuarterlyResult(
                code             = str(r["code"]),
                fiscal_year      = int(r["fiscal_year"]),
                quarter          = int(r["quarter"]),
                cumulative_sales = float(r["cumulative_sales"]),
                cumulative_op    = float(r["cumulative_op"]),
                cumulative_net   = float(r["cumulative_net"]),
                progress_rate    = float(r["progress_rate"]),
            ))
        except (KeyError, ValueError) as e:
            logger.warning(f"[{code}] 履歴レコード変換エラー: {e}")
    return results if len(results) >= MIN_HISTORY_COUNT else []


def save_history(code: str, summary) -> None:
    """
    FinancialSummary を history シートに保存（同一キーは上書き）。
    summary: xbrl_parser.FinancialSummary
    """
    fiscal_year   = int(summary.fiscal_year_end[:4]) if summary.fiscal_year_end else 0
    quarter       = summary.quarter
    progress_rate = (
        round(summary.operating_profit / summary.forecast_op * 100, 1)
        if summary.forecast_op else 0.0
    )
    row = [
        code,
        fiscal_year,
        quarter,
        summary.net_sales,
        summary.operating_profit,
        summary.net_income,
        progress_rate,
        datetime.now().isoformat(),
    ]

    try:
        ws      = _get_sheet(HISTORY_SHEET)
        records = ws.get_all_records()

        for i, r in enumerate(records, start=2):
            if (
                str(r.get("code")) == str(code)
                and int(r.get("fiscal_year", 0)) == fiscal_year
                and int(r.get("quarter", 0)) == quarter
            ):
                ws.update(f"A{i}:H{i}", [row])
                return

        ws.append_row(row)
    except Exception as e:
        logger.error(f"[{code}] history save failed: {e}")


# ── 信用残データの取得 ────────────────────────────────────────────────

def get_margin_data(code: str) -> dict | None:
    try:
        ws      = _get_sheet(MARGIN_SHEET)
        records = ws.get_all_records()
    except Exception as e:
        logger.error(f"[{code}] margin DB fetch failed: {e}")
        return None

    for r in records:
        if str(r.get("code")) == str(code):
            try:
                buy   = float(r["buy"])
                sell  = float(r["sell"])
                ratio = round(buy / sell, 2) if sell > 0 else 999.0
                return {"buy": buy, "sell": sell, "ratio": ratio}
            except (KeyError, ValueError):
                return None
    return None


def save_margin_batch(data: dict[str, dict]) -> None:
    try:
        ws = _get_sheet(MARGIN_SHEET)
        ws.clear()
        ws.append_row(["code", "buy", "sell", "updated_at"])
        now  = datetime.now().isoformat()
        rows = [[code, v["buy"], v["sell"], now] for code, v in data.items()]
        if rows:
            ws.append_rows(rows)
        logger.info(f"信用残データ保存完了: {len(rows)}銘柄")
    except Exception as e:
        logger.error(f"margin save failed: {e}")


# ── 処理済みIDの管理 ──────────────────────────────────────────────────

def load_processed_ids(local_path: str = "data/processed_ids.json") -> set[str]:
    """処理済み開示IDをローカルキャッシュから読み込む。空ならSheetsから補完。"""
    if os.path.exists(local_path):
        try:
            with open(local_path) as f:
                ids = set(json.load(f))
            if ids:
                return ids
        except Exception:
            pass

    try:
        ws      = _get_sheet(PROCESSED_SHEET)
        records = ws.get_all_records()
        ids     = {str(r["doc_id"]) for r in records if r.get("doc_id")}
        logger.info(f"処理済みID {len(ids)}件をSheetsから復元")
        return ids
    except Exception as e:
        logger.warning(f"processed IDs fetch failed: {e}")
        return set()


def save_processed_ids(ids: set[str], local_path: str = "data/processed_ids.json") -> None:
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w") as f:
        json.dump(list(ids), f)

    try:
        ws  = _get_sheet(PROCESSED_SHEET)
        ws.clear()
        ws.append_row(["doc_id", "saved_at"])
        now  = datetime.now().isoformat()
        rows = [[doc_id, now] for doc_id in ids]
        if rows:
            ws.append_rows(rows)
    except Exception as e:
        logger.warning(f"processed IDs Sheets save failed: {e}")
