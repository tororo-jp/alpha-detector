"""
scripts/bulk_import_history.py
=============================
J-Quants API V2を使って過去の財務データを一括取得し、
Google Sheetsのhistoryシートに投入するスクリプト。

【V2変更点】
  - 認証: リフレッシュトークン不要 → APIキー1つで完結
  - エンドポイント: /v1/ → /v2/
  - 環境変数: JQUANTS_REFRESH_TOKEN → JQUANTS_API_KEY
  - レスポンス形式: {"statements": [...]} → {"data": [...]}

【使い方】
  pip install requests gspread google-auth

  JQUANTS_API_KEY='...' \\
  GOOGLE_SHEETS_CREDS='...' \\
  GOOGLE_SHEET_ID='...' \\
  python scripts/bulk_import_history.py

  # 動作確認（10銘柄のみ）
  python scripts/bulk_import_history.py --limit 10
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ── 定数 ──────────────────────────────────────────────
JQUANTS_LISTED_URL = "https://api.jquants.com/v2/listed/info"
JQUANTS_FINS_URL   = "https://api.jquants.com/v2/fins/statements"


def _auth_headers() -> dict:
    """V2 APIキー認証ヘッダーを返す"""
    api_key = os.environ.get("JQUANTS_API_KEY", "")
    if not api_key:
        raise EnvironmentError("JQUANTS_API_KEY が設定されていません")
    return {"x-api-key": api_key}

# 対象市場コード（J-Quants）
MARKET_CODES = {
    "growth"   : ["0109"],          # 東証グロース
    "standard" : ["0111"],          # 東証スタンダード
    "both"     : ["0109", "0111"],  # 両方（デフォルト）
}

SLEEP_BETWEEN_CODES = 0.5   # 銘柄間のsleep（秒）
SLEEP_ON_429        = 60.0  # レート制限時のwait（秒）
MAX_RETRY           = 3

CHECKPOINT_FILE = Path("data/bulk_import_checkpoint.json")

# 取得する財務データのフィールドマッピング
# J-Quants API → historyシートのカラム
FIELD_MAP = {
    "NetSales"                : "cumulative_sales",    # 売上高（円）
    "OperatingProfit"         : "cumulative_op",       # 営業利益（円）
    "NetIncome"               : "cumulative_net",      # 当期純利益（円）
}

# 対象四半期コード
QUARTER_MAP = {"1Q": 1, "2Q": 2, "3Q": 3, "4Q": 4, "FY": 4}

# Google Sheets API設定
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ── 認証 ──────────────────────────────────────────────

def fetch_listed_codes(market_codes: list[str]) -> list[dict]:
    """
    上場銘柄一覧を取得し、対象市場に絞り込む。（V2: APIキー認証）
    """
    res = requests.get(JQUANTS_LISTED_URL, headers=_auth_headers(), timeout=20)
    res.raise_for_status()
    body = res.json()
    # V2レスポンスは {"data": [...]} 形式
    all_stocks = body.get("data") or body.get("info", [])
    logger.info(f"全上場銘柄数: {len(all_stocks)}")

    filtered = [
        {"code": str(s.get("Code", ""))[:4], "name": s.get("CompanyName", "")}
        for s in all_stocks
        if str(s.get("MarketCode", "")) in market_codes
        and s.get("Code")
    ]
    logger.info(f"対象市場銘柄数: {len(filtered)}")
    return filtered
    creds_dict = json.loads(os.environ["GOOGLE_SHEETS_CREDS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ── 財務データ取得 ────────────────────────────────────

def fetch_fins_for_code(code: str, retry: int = 0) -> list[dict]:
    """
    1銘柄の全四半期財務データを取得する。（V2: APIキー認証）
    Returns: J-Quants V2のdataリスト
    """
    try:
        res = requests.get(
            JQUANTS_FINS_URL,
            headers=_auth_headers(),
            params={"code": code},
            timeout=20,
        )

        if res.status_code == 429:
            if retry < MAX_RETRY:
                logger.warning(f"[{code}] 429 Rate limit. {SLEEP_ON_429}秒待機...")
                time.sleep(SLEEP_ON_429)
                return fetch_fins_for_code(code, retry + 1)
            else:
                logger.error(f"[{code}] リトライ上限到達 → スキップ")
                return []

        res.raise_for_status()
        body = res.json()
        # V2レスポンスは {"data": [...]} 形式
        return body.get("data") or body.get("statements", [])

    except requests.RequestException as e:
        logger.warning(f"[{code}] 取得失敗: {e}")
        return []


# ── データ変換 ────────────────────────────────────────

def _to_man_yen(value) -> float | None:
    """円 → 万円変換。Noneや無効値はNoneを返す"""
    if value is None or value == "":
        return None
    try:
        return round(float(value) / 10000, 1)
    except (ValueError, TypeError):
        return None


def _calc_progress_rate(cumulative_op: float | None, forecast_op: float | None) -> float:
    """通期予想に対する累計営業利益の進捗率（%）を計算"""
    if cumulative_op is None or forecast_op is None or forecast_op == 0:
        return 0.0
    return round(cumulative_op / forecast_op * 100, 1)


def statements_to_history_rows(code: str, statements: list[dict]) -> list[dict]:
    """
    J-Quantsのstatementsリストをhistoryシートの行形式に変換する。
    修正開示（TypeOfDocument=='Revision'等）は除外し、
    本決算短信のみを対象とする。

    Returns:
        [
          {
            "code": "1234",
            "fiscal_year": 2023,
            "quarter": 2,
            "cumulative_sales": 5000.0,  # 万円
            "cumulative_op": 300.0,       # 万円
            "cumulative_net": 200.0,      # 万円
            "progress_rate": 42.0,        # %
          }, ...
        ]
    """
    rows = []
    seen_keys = set()  # 重複除去（同一年度・同一Qの複数開示が来た場合）

    for s in statements:
        # 修正開示は除外（本短信のみ対象）
        doc_type = s.get("TypeOfDocument", "")
        if "Revision" in doc_type or "Correction" in doc_type:
            continue

        # 四半期コードを取得
        period = s.get("TypeOfCurrentPeriod", "")
        quarter = QUARTER_MAP.get(period)
        if quarter is None:
            continue

        # 会計年度（西暦）を取得
        fy_str = s.get("FiscalYearEndDate", "")  # 例: "2024-03-31"
        if not fy_str or len(fy_str) < 4:
            continue
        try:
            fiscal_year = int(fy_str[:4])
        except ValueError:
            continue

        # 重複チェック
        key = (fiscal_year, quarter)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        # 財務数値（万円変換）
        cum_sales = _to_man_yen(s.get("NetSales"))
        cum_op    = _to_man_yen(s.get("OperatingProfit"))
        cum_net   = _to_man_yen(s.get("NetIncome") or s.get("Profit"))
        fy_op     = _to_man_yen(s.get("ForecastOperatingProfit"))

        # 主要数値が欠損の行は除外
        if cum_op is None or cum_sales is None:
            continue

        progress_rate = _calc_progress_rate(cum_op, fy_op)

        rows.append({
            "code"            : code,
            "fiscal_year"     : fiscal_year,
            "quarter"         : quarter,
            "cumulative_sales": cum_sales if cum_sales is not None else 0.0,
            "cumulative_op"   : cum_op,
            "cumulative_net"  : cum_net if cum_net is not None else 0.0,
            "progress_rate"   : progress_rate,
        })

    return rows


# ── Google Sheets書き込み ─────────────────────────────

def save_rows_to_sheets(rows: list[dict], ws: gspread.Worksheet) -> None:
    """
    historyシートに行を追記する。
    大量データなのでappend_rowsで一括書き込み。
    """
    if not rows:
        return

    now = datetime.now().isoformat()
    sheet_rows = [
        [
            r["code"],
            r["fiscal_year"],
            r["quarter"],
            r["cumulative_sales"],
            r["cumulative_op"],
            r["cumulative_net"],
            r["progress_rate"],
            now,
        ]
        for r in rows
    ]
    ws.append_rows(sheet_rows, value_input_option="RAW")


# ── チェックポイント管理 ─────────────────────────────

def load_checkpoint() -> set[str]:
    """処理済み銘柄コードを読み込む（中断再開用）"""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            return set(json.load(f))
    return set()


def save_checkpoint(done_codes: set[str]) -> None:
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(list(done_codes), f)


# ── メイン処理 ────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Alpha-Detector 過去財務データ一括投入")
    parser.add_argument(
        "--market",
        choices=["growth", "standard", "both"],
        default="both",
        help="対象市場（デフォルト: both）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="処理銘柄数の上限（デフォルト: 0=全件）。動作確認時に10など指定",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="チェックポイントをリセットして最初から再実行",
    )
    args = parser.parse_args()

    # 環境変数チェック
    required_envs = ["JQUANTS_API_KEY", "GOOGLE_SHEETS_CREDS", "GOOGLE_SHEET_ID"]
    missing = [e for e in required_envs if not os.environ.get(e)]
    if missing:
        print(f"❌ 環境変数が不足しています: {', '.join(missing)}")
        sys.exit(1)

    # チェックポイント
    if args.reset_checkpoint and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logger.info("チェックポイントをリセットしました")
    done_codes = load_checkpoint()
    logger.info(f"処理済み銘柄（前回分）: {len(done_codes)}件")

    # APIキー確認
    logger.info("J-Quants APIキーを確認中...")
    _auth_headers()  # キーが未設定なら例外を投げる

    logger.info("Google Sheetsに接続中...")
    sheets_client = get_sheets_client()
    spreadsheet = sheets_client.open_by_key(os.environ["GOOGLE_SHEET_ID"])
    history_ws = spreadsheet.worksheet("history")

    # 銘柄一覧取得
    target_market_codes = MARKET_CODES[args.market]
    all_stocks = fetch_listed_codes(target_market_codes)

    # 未処理銘柄のみに絞り込み
    pending = [s for s in all_stocks if s["code"] not in done_codes]
    if args.limit > 0:
        pending = pending[:args.limit]
    logger.info(f"処理対象: {len(pending)}銘柄（スキップ済み: {len(done_codes)}銘柄）")

    # バッファ（Sheetsへの書き込みを50銘柄ごとにまとめる）
    BATCH_SIZE = 50
    buffer: list[dict] = []
    success_count = 0
    skip_count = 0

    for i, stock in enumerate(pending):
        code = stock["code"]
        name = stock["name"]

        logger.info(f"[{i+1}/{len(pending)}] {code} {name} を処理中...")

        # 財務データ取得（V2: トークン不要）
        statements = fetch_fins_for_code(code)
        if not statements:
            logger.warning(f"  → データなし（スキップ）")
            skip_count += 1
            done_codes.add(code)
            time.sleep(SLEEP_BETWEEN_CODES)
            continue

        # 行形式に変換
        rows = statements_to_history_rows(code, statements)
        logger.info(f"  → {len(rows)}件の四半期データを取得")

        buffer.extend(rows)
        success_count += 1
        done_codes.add(code)

        # バッファがBATCH_SIZEを超えたらSheetsに書き込み
        if len(buffer) >= BATCH_SIZE * 4:  # 1銘柄最大4四半期 × 50銘柄
            logger.info(f"  Sheetsに{len(buffer)}件を書き込み中...")
            save_rows_to_sheets(buffer, history_ws)
            buffer.clear()
            save_checkpoint(done_codes)

        time.sleep(SLEEP_BETWEEN_CODES)

    # 残りバッファを書き込み
    if buffer:
        logger.info(f"残り{len(buffer)}件をSheetsに書き込み中...")
        save_rows_to_sheets(buffer, history_ws)

    # チェックポイント保存
    save_checkpoint(done_codes)

    # 完了メッセージ
    print("\n" + "="*50)
    print(f"✅ 完了！")
    print(f"   成功: {success_count}銘柄")
    print(f"   スキップ: {skip_count}銘柄")
    print(f"   合計投入行数: 約{success_count * 3}〜{success_count * 8}件")
    print("="*50)
    print()
    print("⚠️  J-Quants無料プランご利用の場合:")
    print("   過去2年分のデータが投入されました（12週遅延）。")
    print("   システム稼働後、毎決算期に自動蓄積されるため、")
    print("   3年目以降は自動的に3年分のデータが揃います。")


if __name__ == "__main__":
    main()
