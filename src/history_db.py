"""
history_db.py
Google Sheetsを財務履歴DBとして使用するモジュール。

シート構成:
  - "history"  : 過去3年分の四半期財務データ
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

HISTORY_SHEET  = "history"
MARGIN_SHEET   = "margin"
PROCESSED_SHEET = "processed"

# 過去3年のデータが揃っているとみなす最低件数
MIN_HISTORY_COUNT = 3


@dataclass
class QuarterlyResult:
    code: str
    fiscal_year: int
    quarter: int            # 1〜4
    cumulative_sales: float # 累計売上高（万円）
    cumulative_op: float    # 累計営業利益（万円）
    cumulative_net: float   # 累計純利益（万円）
    progress_rate: float    # 通期予想比進捗率（%）


def _get_client() -> gspread.Client:
    creds_json = os.environ["GOOGLE_SHEETS_CREDS"]
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def _get_sheet(sheet_name: str) -> gspread.Worksheet:
    client = _get_client()
    sheet_id = os.environ["GOOGLE_SHEET_ID"]
    spreadsheet = client.open_by_key(sheet_id)
    return spreadsheet.worksheet(sheet_name)


# ──────────────────────────────────────────────
# 財務履歴の取得
# ──────────────────────────────────────────────

def get_history(code: str, quarter: int) -> list[QuarterlyResult]:
    """
    指定銘柄・四半期の過去履歴を返す。
    Returns:
        過去3年分(最大)のQuarterlyResultリスト。
        件数がMIN_HISTORY_COUNT未満の場合は空リストを返す（除外対象）。
    """
    try:
        ws = _get_sheet(HISTORY_SHEET)
        records = ws.get_all_records()
    except Exception as e:
        logger.error(f"[{code}] history DB fetch failed: {e}")
        return []

    matched = [
        r for r in records
        if str(r.get("code")) == str(code)
        and int(r.get("quarter", 0)) == quarter
    ]

    # 古い順にソートして直近3件を取得
    matched.sort(key=lambda r: r.get("fiscal_year", 0))
    matched = matched[-3:]

    if len(matched) < MIN_HISTORY_COUNT:
        logger.info(f"[{code}] 過去3年の履歴不足（{len(matched)}件）→ 除外")
        return []

    results = []
    for r in matched:
        try:
            results.append(QuarterlyResult(
                code=str(r["code"]),
                fiscal_year=int(r["fiscal_year"]),
                quarter=int(r["quarter"]),
                cumulative_sales=float(r["cumulative_sales"]),
                cumulative_op=float(r["cumulative_op"]),
                cumulative_net=float(r["cumulative_net"]),
                progress_rate=float(r["progress_rate"]),
            ))
        except (KeyError, ValueError) as e:
            logger.warning(f"[{code}] 履歴レコード変換エラー: {e} / row={r}")
            continue

    # 変換後も件数チェック
    if len(results) < MIN_HISTORY_COUNT:
        logger.info(f"[{code}] 変換後の履歴不足（{len(results)}件）→ 除外")
        return []

    return results


def save_history(result: QuarterlyResult) -> None:
    """財務履歴をGoogle Sheetsに保存（同一キーは上書き）"""
    try:
        ws = _get_sheet(HISTORY_SHEET)
        records = ws.get_all_records()

        # 同一レコードを探して上書き
        for i, r in enumerate(records, start=2):  # 1行目はヘッダー
            if (
                str(r.get("code")) == result.code
                and int(r.get("fiscal_year", 0)) == result.fiscal_year
                and int(r.get("quarter", 0)) == result.quarter
            ):
                ws.update(
                    f"A{i}:H{i}",
                    [[
                        result.code,
                        result.fiscal_year,
                        result.quarter,
                        result.cumulative_sales,
                        result.cumulative_op,
                        result.cumulative_net,
                        result.progress_rate,
                        datetime.now().isoformat(),
                    ]]
                )
                return

        # 新規追加
        ws.append_row([
            result.code,
            result.fiscal_year,
            result.quarter,
            result.cumulative_sales,
            result.cumulative_op,
            result.cumulative_net,
            result.progress_rate,
            datetime.now().isoformat(),
        ])
    except Exception as e:
        logger.error(f"[{result.code}] history save failed: {e}")


# ──────────────────────────────────────────────
# 信用残データの取得
# ──────────────────────────────────────────────

def get_margin_data(code: str) -> dict | None:
    """
    最新週次の信用残データを返す。
    Returns:
        {"buy": 買い残株数, "sell": 売り残株数, "ratio": 信用倍率} or None
    """
    try:
        ws = _get_sheet(MARGIN_SHEET)
        records = ws.get_all_records()
    except Exception as e:
        logger.error(f"[{code}] margin DB fetch failed: {e}")
        return None

    for r in records:
        if str(r.get("code")) == str(code):
            try:
                buy  = float(r["buy"])
                sell = float(r["sell"])
                ratio = round(buy / sell, 2) if sell > 0 else 999.0
                return {"buy": buy, "sell": sell, "ratio": ratio}
            except (KeyError, ValueError, ZeroDivisionError):
                return None
    return None


def save_margin_batch(data: dict[str, dict]) -> None:
    """信用残データを一括保存（全件置き換え）"""
    try:
        ws = _get_sheet(MARGIN_SHEET)
        ws.clear()
        ws.append_row(["code", "buy", "sell", "updated_at"])
        now = datetime.now().isoformat()
        rows = [
            [code, v["buy"], v["sell"], now]
            for code, v in data.items()
        ]
        if rows:
            ws.append_rows(rows)
        logger.info(f"信用残データ保存完了: {len(rows)}銘柄")
    except Exception as e:
        logger.error(f"margin save failed: {e}")


# ──────────────────────────────────────────────
# 処理済みIDの管理
# ──────────────────────────────────────────────

def load_processed_ids(local_path: str = "data/processed_ids.json") -> set[str]:
    """
    処理済み開示IDをローカルキャッシュから読み込む。
    ローカルが空の場合はGoogle Sheetsから補完（7日リセット対策）。
    """
    ids: set[str] = set()

    # ローカルキャッシュから読み込み
    if os.path.exists(local_path):
        try:
            with open(local_path) as f:
                ids = set(json.load(f))
            if ids:
                return ids
        except Exception:
            pass

    # ローカルが空 → Sheetsから補完
    try:
        ws = _get_sheet(PROCESSED_SHEET)
        records = ws.get_all_records()
        ids = {str(r["doc_id"]) for r in records if r.get("doc_id")}
        logger.info(f"処理済みID {len(ids)}件をSheetsから復元")
    except Exception as e:
        logger.warning(f"processed IDs Sheets fetch failed: {e}")

    return ids


def save_processed_ids(ids: set[str], local_path: str = "data/processed_ids.json") -> None:
    """処理済みIDをローカルとGoogle Sheetsの両方に保存"""
    # ローカル保存
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w") as f:
        json.dump(list(ids), f)

    # Sheets保存（全件置き換え）
    try:
        ws = _get_sheet(PROCESSED_SHEET)
        ws.clear()
        ws.append_row(["doc_id", "saved_at"])
        now = datetime.now().isoformat()
        rows = [[doc_id, now] for doc_id in ids]
        if rows:
            ws.append_rows(rows)
    except Exception as e:
        logger.warning(f"processed IDs Sheets save failed: {e}")
