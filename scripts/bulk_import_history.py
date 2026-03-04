"""
scripts/bulk_import_history.py
==============================
J-Quants API V2（Freeプラン）を使って過去の財務データを一括取得し、
Google Sheetsのhistoryシートに投入するスクリプト。

【Freeプランの制約と対応】
  - /fins/statements（銘柄コード指定）→ Lightプラン以上が必要 → 使用しない
  - /fins/summary（日付指定）         → Freeプランで利用可能  → これを使用
  - Freeプランのデータは12週間遅延で配信されます
    ※ 過去データの初期投入には問題なし
    ※ リアルタイム検知にはLightプラン以上が必要（本番稼働時）

【取得方式】
  「日付を1日ずつループして /fins/summary を叩く」方式。
  1リクエストで「その日に開示された全銘柄分」が取れるため、
  リクエスト数は日数分（デフォルト3年 ≒ 750回）。

【使い方（PowerShell）】
  $env:JQUANTS_API_KEY    = "your-api-key"
  $env:GOOGLE_SHEET_ID    = "your-sheet-id"
  $env:GOOGLE_SHEETS_CREDS = Get-Clipboard

  # 動作確認（直近7日間のみ）
  python scripts/bulk_import_history.py --days 7

  # 全件投入（デフォルト：過去3年分）
  python scripts/bulk_import_history.py

  # 中断後の再開（そのまま再実行するだけ）
  python scripts/bulk_import_history.py

  # 最初からやり直し
  python scripts/bulk_import_history.py --reset-checkpoint

【処理時間の目安】
  3年分（約750営業日）× 1秒/リクエスト ≒ 約15〜20分
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, date
from pathlib import Path

import requests
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ── 定数 ──────────────────────────────────────────────────
JQUANTS_SUMMARY_URL = "https://api.jquants.com/v2/fins/summary"

MARKET_CODES = {
    "growth"  : {"0109"},
    "standard": {"0111"},
    "both"    : {"0109", "0111"},
}

SLEEP_BETWEEN_REQUESTS = 1.0
SLEEP_ON_429           = 60.0
MAX_RETRY              = 3
DEFAULT_DAYS           = 365 * 3

QUARTER_MAP = {"1Q": 1, "2Q": 2, "3Q": 3, "4Q": 4, "FY": 4}

CHECKPOINT_FILE = Path("data/bulk_import_checkpoint.json")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _auth_headers() -> dict:
    api_key = os.environ.get("JQUANTS_API_KEY", "")
    if not api_key:
        raise EnvironmentError("JQUANTS_API_KEY が設定されていません")
    return {"x-api-key": api_key}


def get_sheets_client() -> gspread.Client:
    creds_dict = json.loads(os.environ["GOOGLE_SHEETS_CREDS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def _business_days(start: date, end: date) -> list[date]:
    """start〜end間の営業日（月〜金）を新しい日から順に返す"""
    days = []
    d = end
    while d >= start:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return days


def fetch_summary_by_date(target_date: date, retry: int = 0) -> list[dict]:
    """指定日に開示された全銘柄の決算サマリーを取得する"""
    date_str = target_date.strftime("%Y%m%d")
    try:
        res = requests.get(
            JQUANTS_SUMMARY_URL,
            headers=_auth_headers(),
            params={"date": date_str},
            timeout=20,
        )
        if res.status_code == 429:
            if retry < MAX_RETRY:
                logger.warning(f"[{date_str}] 429 Rate limit. {SLEEP_ON_429}秒待機...")
                time.sleep(SLEEP_ON_429)
                return fetch_summary_by_date(target_date, retry + 1)
            logger.error(f"[{date_str}] リトライ上限到達 → スキップ")
            return []
        if res.status_code == 404:
            return []
        res.raise_for_status()
        body = res.json()
        return body.get("data") or body.get("statements", [])
    except requests.RequestException as e:
        logger.warning(f"[{date_str}] 取得失敗: {e}")
        return []


def _to_man_yen(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return round(float(value) / 10000, 1)
    except (ValueError, TypeError):
        return None


def _calc_progress_rate(cum_op: float | None, fy_op: float | None) -> float:
    if cum_op is None or fy_op is None or fy_op == 0:
        return 0.0
    return round(cum_op / fy_op * 100, 1)


def summaries_to_history_rows(
    records: list[dict],
    target_market_codes: set[str],
) -> list[dict]:
    """
    /fins/summary のレスポンスリストを historyシート用の行形式に変換する。
    対象市場・修正報告除外・欠損除外を行う。
    """
    rows = []
    seen_keys: set[tuple] = set()

    for s in records:
        # 市場フィルタ
        if str(s.get("MarketCode", "")) not in target_market_codes:
            continue

        # 修正報告書を除外
        doc_type = s.get("TypeOfDocument", "")
        if "Amendment" in doc_type or "Revision" in doc_type:
            continue

        # 四半期コード
        quarter = QUARTER_MAP.get(s.get("TypeOfCurrentPeriod", ""))
        if quarter is None:
            continue

        # 銘柄コード
        code = str(s.get("Code", ""))[:4]
        if not code or not code.isdigit():
            continue

        # 会計年度
        fy_str = s.get("CurrentFiscalYearEndDate", "") or s.get("FiscalYearEndDate", "")
        if not fy_str or len(fy_str) < 4:
            continue
        try:
            fiscal_year = int(fy_str[:4])
        except ValueError:
            continue

        # 重複チェック
        key = (code, fiscal_year, quarter)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        # 財務数値（万円換算）
        cum_sales = _to_man_yen(s.get("NetSales"))
        cum_op    = _to_man_yen(s.get("OperatingProfit"))
        cum_net   = _to_man_yen(s.get("Profit") or s.get("NetIncome"))
        fy_op     = _to_man_yen(s.get("ForecastOperatingProfit"))

        if cum_op is None or cum_sales is None:
            continue

        rows.append({
            "code"            : code,
            "fiscal_year"     : fiscal_year,
            "quarter"         : quarter,
            "cumulative_sales": cum_sales,
            "cumulative_op"   : cum_op,
            "cumulative_net"  : cum_net if cum_net is not None else 0.0,
            "progress_rate"   : _calc_progress_rate(cum_op, fy_op),
        })

    return rows


def save_rows_to_sheets(rows: list[dict], ws: gspread.Worksheet) -> None:
    if not rows:
        return
    now = datetime.now().isoformat()
    sheet_rows = [
        [
            r["code"], r["fiscal_year"], r["quarter"],
            r["cumulative_sales"], r["cumulative_op"],
            r["cumulative_net"], r["progress_rate"], now,
        ]
        for r in rows
    ]
    ws.append_rows(sheet_rows, value_input_option="RAW")


def load_checkpoint() -> set[str]:
    """処理済み日付（YYYYMMDD）のセットを返す"""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE) as f:
                return set(json.load(f))
        except Exception:
            pass
    return set()


def save_checkpoint(done_dates: set[str]) -> None:
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(list(done_dates), f)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Alpha-Detector 過去財務データ一括投入（日付ループ方式）"
    )
    parser.add_argument(
        "--market", choices=["growth", "standard", "both"], default="both",
    )
    parser.add_argument(
        "--days", type=int, default=DEFAULT_DAYS,
        help=f"取得する過去日数（デフォルト: {DEFAULT_DAYS}日 ≒ 3年）",
    )
    parser.add_argument(
        "--reset-checkpoint", action="store_true",
        help="チェックポイントをリセットして最初から再実行",
    )
    args = parser.parse_args()

    required_envs = ["JQUANTS_API_KEY", "GOOGLE_SHEETS_CREDS", "GOOGLE_SHEET_ID"]
    missing = [e for e in required_envs if not os.environ.get(e)]
    if missing:
        print(f"❌ 環境変数が不足しています: {', '.join(missing)}")
        sys.exit(1)

    if args.reset_checkpoint and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logger.info("チェックポイントをリセットしました")
    done_dates = load_checkpoint()
    logger.info(f"処理済み日付（前回分）: {len(done_dates)}件")

    _auth_headers()  # APIキー確認

    logger.info("Google Sheetsに接続中...")
    history_ws = get_sheets_client().open_by_key(
        os.environ["GOOGLE_SHEET_ID"]
    ).worksheet("history")

    end_date   = date.today()
    start_date = end_date - timedelta(days=args.days)
    all_days   = _business_days(start_date, end_date)
    pending    = [d for d in all_days if d.strftime("%Y%m%d") not in done_dates]
    target_codes = MARKET_CODES[args.market]

    logger.info(
        f"取得対象: {start_date} 〜 {end_date} "
        f"（{len(all_days)}営業日 / 未処理: {len(pending)}日）"
    )

    buffer: list[dict] = []
    total_rows = 0

    for i, target_date in enumerate(pending):
        date_str = target_date.strftime("%Y%m%d")
        records  = fetch_summary_by_date(target_date)

        if records:
            rows = summaries_to_history_rows(records, target_codes)
            buffer.extend(rows)
            logger.info(
                f"[{i+1}/{len(pending)}] {date_str}: "
                f"{len(records)}件取得 → {len(rows)}件変換"
            )
        else:
            logger.info(f"[{i+1}/{len(pending)}] {date_str}: データなし")

        done_dates.add(date_str)

        if len(buffer) >= 100:
            logger.info(f"  Sheetsに{len(buffer)}件書き込み中...")
            save_rows_to_sheets(buffer, history_ws)
            total_rows += len(buffer)
            buffer.clear()
            save_checkpoint(done_dates)

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    if buffer:
        logger.info(f"残り{len(buffer)}件をSheetsに書き込み中...")
        save_rows_to_sheets(buffer, history_ws)
        total_rows += len(buffer)

    save_checkpoint(done_dates)

    print("\n" + "=" * 50)
    print("✅ 完了！")
    print(f"   処理日数  : {len(pending)}営業日")
    print(f"   投入行数  : {total_rows}件")
    print("=" * 50)
    print()
    print("⚠️  Freeプランご利用の場合:")
    print("   データは12週間遅延のため、直近3ヶ月分は欠損します。")
    print("   リアルタイム検知にはLightプラン以上が必要です。")


if __name__ == "__main__":
    main()
