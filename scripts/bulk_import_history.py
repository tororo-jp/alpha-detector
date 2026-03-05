"""
scripts/bulk_import_history.py
==============================
TDnet の銘柄別JSON（非公式・安定稼働）を使って
過去5年分の決算短信XBRLを一括取得し、
Google Sheetsのhistoryシートに投入するスクリプト。

【データ取得元】
  https://www.release.tdnet.info/inbs/I_list_00.json?Sccode={code}&Sort=1
  ・銘柄コード指定で過去5年分の開示一覧がJSONで返る
  ・J-Quants不要・完全無料・リアルタイム検知と同一のXBRLパース処理を使用

【銘柄一覧の取得方式（自動フォールバック）】
  ① JPX公式XLS（pandas が xlrd/openpyxl で読める場合）
      → 市場区分フィルタ済みで正確・高速
  ② TDnetコード総当たり（①が失敗した場合の自動フォールバック）
      → 1000〜9999を順番にTDnetに問い合わせ、開示があるコードだけ処理
      → 存在しないコードは0.3秒sleepで即スキップするため速度は許容範囲

【使い方（PowerShell）】
  $env:GOOGLE_SHEET_ID    = "your-sheet-id"
  $env:GOOGLE_SHEETS_CREDS = Get-Clipboard   # JSONキーをコピー済みの場合

  # 動作確認（1銘柄のみ）
  python scripts/bulk_import_history.py --test-code 7203

  # 全件投入（デフォルト：東証グロース+スタンダード）
  python scripts/bulk_import_history.py

  # 中断後の再開（そのまま再実行するだけ）
  python scripts/bulk_import_history.py

  # 最初からやり直し
  python scripts/bulk_import_history.py --reset-checkpoint

【処理時間の目安】
  方式①（JPX XLS）: 約2〜4時間（グロース+スタンダード 約3,000社）
  方式②（総当たり）: 約1〜2時間追加（空振り分の0.3秒sleepが加わる）
  ※ 途中中断→再開に対応（checkpointに処理済み銘柄を保存）
"""

import json
import logging
import os
import sys
import time
import random
from datetime import datetime
from pathlib import Path

import requests
import gspread
from google.oauth2.service_account import Credentials

# srcディレクトリをパスに追加してxbrl_parserを使い回す
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from xbrl_parser import parse_disclosure, FinancialSummary

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── 定数 ──────────────────────────────────────────────────────────────────
TDNET_JSON_URL   = "https://www.release.tdnet.info/inbs/I_list_00.json?Sccode={code}&Sort=1"
TDNET_LISTED_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

# 東証グロース・スタンダードの銘柄コード範囲
# 内国株は1000〜9999。プライムを除いた中小型が多い帯域をカバー
CODE_RANGE_START = 1000
CODE_RANGE_END   = 9999

# 対象市場コード（J-Quants市場コードと同じ仕様）
MARKET_CODES = {
    "growth"  : {"0109"},
    "standard": {"0111"},
    "both"    : {"0109", "0111"},
}

SLEEP_BETWEEN_CODES = 2.0   # 銘柄間のsleep（秒）
SLEEP_BETWEEN_DOCS  = 1.5   # 同一銘柄内の文書間sleep（秒）
SLEEP_ON_429        = 60.0
MAX_RETRY           = 3

CHECKPOINT_FILE = Path("data/bulk_import_checkpoint.json")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# 決算短信キーワード（業績修正・配当修正も含める）
TARGET_KEYWORDS = ["決算短信", "業績予想の修正", "配当予想の修正"]


# ── 認証 ──────────────────────────────────────────────────────────────────

def get_sheets_client() -> gspread.Client:
    creds_dict = json.loads(os.environ["GOOGLE_SHEETS_CREDS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ── 銘柄一覧取得（JPX上場銘柄CSV）────────────────────────────────────────

def fetch_listed_codes(market: str) -> list[dict]:
    """
    東証グロース・スタンダードの上場銘柄コード一覧を取得する。

    取得方式（優先順）:
      ① JPX公式XLS（pandas + xlrd が使える場合）
      ② JPX公式XLS（openpyxl で代替できる場合）
      ③ TDnetの銘柄別JSONに存在するコードをコード範囲総当たりで収集

    どの方式でも同じ list[{"code": str}] 形式で返す。
    """
    # ── 方式①②: JPX公式XLS ─────────────────────────────────────────────
    codes = _fetch_from_jpx_xls(market)
    if codes:
        return codes

    # ── 方式③: TDnet総当たり（フォールバック）──────────────────────────
    logger.info("JPX XLS取得不可 → TDnetコード総当たり方式にフォールバックします")
    logger.info(f"コード範囲 {CODE_RANGE_START}〜{CODE_RANGE_END} を順番に確認します")
    logger.info("（TDnetに開示履歴があるコードのみ処理対象になります）")
    return [{"code": str(c).zfill(4)} for c in range(CODE_RANGE_START, CODE_RANGE_END + 1)]


def _fetch_from_jpx_xls(market: str) -> list[dict]:
    """
    JPX公式XLS（data_j.xls）から市場フィルタ済みの銘柄コード一覧を返す。
    取得失敗時は空リストを返す。
    """
    import pandas as pd

    # グロース・スタンダードの市場区分名（XLS内の表記）
    market_keywords = {
        "growth"  : ["グロース"],
        "standard": ["スタンダード"],
        "both"    : ["グロース", "スタンダード"],
    }.get(market, ["グロース", "スタンダード"])
    pattern = "|".join(market_keywords)

    # xlrd（旧XLS用）と openpyxl（新XLSX用）の両方を試す
    for engine in [None, "openpyxl", "xlrd"]:
        try:
            kwargs = {"header": 0}
            if engine:
                kwargs["engine"] = engine
            logger.info(f"JPX上場銘柄一覧を取得中... (engine={engine or 'auto'})")
            df = pd.read_excel(TDNET_LISTED_URL, **kwargs)

            code_cols   = [c for c in df.columns if "コード" in str(c)]
            market_cols = [c for c in df.columns if "市場" in str(c)]
            if not code_cols or not market_cols:
                continue

            filtered = df[df[market_cols[0]].astype(str).str.contains(pattern, na=False)]
            codes = [
                {"code": str(int(c)).zfill(4)}
                for c in filtered[code_cols[0]].dropna()
                if str(c).replace(".0", "").isdigit()
            ]
            if codes:
                logger.info(f"JPX一覧から {len(codes)} 銘柄を取得")
                return codes
        except ImportError:
            continue   # engineが入っていない場合はスキップ
        except Exception as e:
            logger.warning(f"JPX XLS取得失敗 (engine={engine}): {e}")
            continue

    return []


# ── TDnet銘柄別JSON取得 ───────────────────────────────────────────────────

def fetch_xbrl_urls_for_code(code: str, retry: int = 0) -> list[dict]:
    """
    TDnetの銘柄別JSON（非公式）から決算短信のXBRL URL一覧を取得する。

    Returns:
        [{"doc_id": str, "title": str, "xbrl_zip_url": str, "code": str}, ...]
    """
    url = TDNET_JSON_URL.format(code=code)
    try:
        res = requests.get(url, timeout=15)
        if res.status_code == 429:
            if retry < MAX_RETRY:
                logger.warning(f"[{code}] 429 Rate limit. {SLEEP_ON_429}秒待機...")
                time.sleep(SLEEP_ON_429)
                return fetch_xbrl_urls_for_code(code, retry + 1)
            return []
        if res.status_code == 404:
            return []
        res.raise_for_status()
        data = res.json()
    except Exception as e:
        logger.warning(f"[{code}] TDnet JSON取得失敗: {e}")
        return []

    results = []
    for item in data.get("list", []):
        title     = item.get("title", "")
        file_link = item.get("fileLink", "")

        # 決算短信・修正のみ、訂正は除外
        if not any(kw in title for kw in TARGET_KEYWORDS):
            continue
        if "訂正" in title:
            continue
        if not file_link or not file_link.endswith(".zip"):
            continue

        doc_id = file_link.replace(".zip", "").replace(".ZIP", "")
        # filelinkはパスのみの場合があるのでベースURLを付与
        if not file_link.startswith("http"):
            file_link = "https://www.release.tdnet.info/inbs/" + file_link

        results.append({
            "doc_id"      : doc_id,
            "title"       : title,
            "xbrl_zip_url": file_link,
            "code"        : code,
            "company_name": item.get("company", ""),
        })

    return results


# ── Google Sheets書き込み ─────────────────────────────────────────────────

def save_summary_to_sheets(summary: FinancialSummary, ws: gspread.Worksheet) -> None:
    """FinancialSummaryをhistoryシートに1行追記"""
    ws.append_rows([[
        summary.code,
        summary.fiscal_year,
        summary.quarter,
        summary.cumulative_sales,
        summary.cumulative_op,
        summary.cumulative_net,
        summary.progress_rate,
        datetime.now().isoformat(),
    ]], value_input_option="RAW")


# ── チェックポイント管理 ──────────────────────────────────────────────────

def load_checkpoint() -> set[str]:
    """処理済み銘柄コードのセットを返す"""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE) as f:
                return set(json.load(f))
        except Exception:
            pass
    return set()


def save_checkpoint(done_codes: set[str]) -> None:
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(list(done_codes), f)


# ── メイン ────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Alpha-Detector 過去財務データ一括投入（TDnet方式）")
    parser.add_argument("--market", choices=["growth", "standard", "both"], default="both")
    parser.add_argument("--test-code", type=str, default="",
                        help="動作確認用：指定した1銘柄だけ処理（Sheetsへの書き込みなし）")
    parser.add_argument("--codes", type=str, default="",
                        help="カンマ区切りで銘柄コードを直接指定（例: 1234,5678）")
    parser.add_argument("--reset-checkpoint", action="store_true",
                        help="チェックポイントをリセットして最初から再実行")
    args = parser.parse_args()

    # テストモード
    if args.test_code:
        logger.info(f"テストモード: {args.test_code}")
        docs = fetch_xbrl_urls_for_code(args.test_code)
        logger.info(f"{len(docs)}件の対象開示を発見")
        for d in docs[:3]:  # 最大3件だけ確認
            logger.info(f"  {d['title']} → {d['xbrl_zip_url']}")
            summary = parse_disclosure(d)
            if summary:
                print(f"\n{'='*50}")
                print(f"コード    : {summary.code}")
                print(f"決算期末  : {summary.fiscal_year_end}  ({summary.fiscal_year}年度)")
                print(f"四半期    : {summary.quarter}Q")
                print(f"売上高    : {summary.cumulative_sales:,.0f}万円")
                print(f"営業利益  : {summary.cumulative_op:,.0f}万円")
                print(f"純利益    : {summary.cumulative_net:,.0f}万円")
                print(f"通期予想  : {summary.forecast_op}万円")
                print(f"{'='*50}\n")
            time.sleep(SLEEP_BETWEEN_DOCS)
        return

    # 通常モード
    required_envs = ["GOOGLE_SHEETS_CREDS", "GOOGLE_SHEET_ID"]
    missing = [e for e in required_envs if not os.environ.get(e)]
    if missing:
        print(f"❌ 環境変数が不足しています: {', '.join(missing)}")
        sys.exit(1)

    if args.reset_checkpoint and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logger.info("チェックポイントをリセットしました")
    done_codes = load_checkpoint()
    logger.info(f"処理済み銘柄（前回分）: {len(done_codes)}件")

    logger.info("Google Sheetsに接続中...")
    history_ws = get_sheets_client().open_by_key(
        os.environ["GOOGLE_SHEET_ID"]
    ).worksheet("history")

    # 銘柄一覧取得
    if args.codes:
        stocks = [{"code": c.strip()} for c in args.codes.split(",")]
    else:
        stocks = fetch_listed_codes(args.market)

    if not stocks:
        print("❌ 銘柄一覧が取得できませんでした。--codes オプションで直接指定してください。")
        sys.exit(1)

    pending = [s for s in stocks if s["code"] not in done_codes]
    logger.info(f"処理対象: {len(pending)}銘柄 / スキップ済み: {len(done_codes)}銘柄")

    total_rows  = 0
    skip_count  = 0

    for i, stock in enumerate(pending):
        code = stock["code"]
        logger.info(f"[{i+1}/{len(pending)}] {code} の過去データを取得中...")

        docs = fetch_xbrl_urls_for_code(code)
        if not docs:
            logger.debug(f"  → [{code}] 開示なし（スキップ）")
            skip_count += 1
            done_codes.add(code)
            # 総当たり時は空振りが多いので短いsleepで効率化
            time.sleep(0.3)
            continue

        saved = 0
        for doc in docs:
            summary = parse_disclosure(doc)
            if summary and summary.cumulative_op is not None:
                save_summary_to_sheets(summary, history_ws)
                saved += 1
                total_rows += 1
            time.sleep(SLEEP_BETWEEN_DOCS)

        logger.info(f"  → {len(docs)}件中 {saved}件保存")
        done_codes.add(code)

        # 50銘柄ごとにチェックポイント保存
        if (i + 1) % 50 == 0:
            save_checkpoint(done_codes)

        time.sleep(SLEEP_BETWEEN_CODES + random.uniform(0, 1.0))

    save_checkpoint(done_codes)

    print("\n" + "=" * 50)
    print("✅ 完了！")
    print(f"   処理銘柄数 : {len(pending) - skip_count}銘柄")
    print(f"   スキップ   : {skip_count}銘柄")
    print(f"   投入行数   : {total_rows}件")
    print("=" * 50)


if __name__ == "__main__":
    main()
