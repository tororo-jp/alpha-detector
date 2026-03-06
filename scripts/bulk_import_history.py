"""
scripts/bulk_import_history.py
==============================
TDnetの日付指定HTMLページを過去3年分ループして
決算短信XBRLを一括取得し、Google Sheetsのhistoryシートに投入する。

【なぜ日付ループ方式か】
  TDnetには「銘柄コード指定で過去データを取得できる公開URL」が存在しない。
  唯一動作が確認できているのは日付指定のURL:
    https://www.release.tdnet.info/inbs/I_list_001_YYYYMMDD.html
  1日分のリクエストで「その日に発表された全銘柄の開示」がまとめて取れる。
  これを過去3年分（約780営業日）繰り返すことで全銘柄の履歴を収集する。

【処理フロー】
  過去3年分の営業日リストを生成
    ↓ 1日ずつループ
  I_list_001_YYYYMMDD.html をスクレイピング（複数ページ対応）
    ↓ 決算短信・業績修正・配当修正のみ抽出
  XBRL ZIPをダウンロード・パース（xbrl_parser.py を使い回し）
    ↓
  Google Sheetsのhistoryシートに追記
    ↓
  チェックポイント保存（中断→再開に対応）

【使い方（PowerShell）】
  $env:GOOGLE_SHEET_ID    = "your-sheet-id"
  $env:GOOGLE_SHEETS_CREDS = Get-Clipboard

  # 動作確認（直近5営業日のみ・Sheetsへの書き込みなし）
  python scripts/bulk_import_history.py --dry-run --days 7

  # 全件投入（デフォルト：過去3年分）
  python scripts/bulk_import_history.py

  # 中断後の再開（そのまま再実行するだけ）
  python scripts/bulk_import_history.py

  # 最初からやり直し
  python scripts/bulk_import_history.py --reset-checkpoint

【処理時間の目安】
  3年分 ≒ 780営業日
  1日あたり: HTML取得(複数ページ) + XBRL DL・パース × 発表件数
  決算集中日（2月・3月・5月・8月・11月）は1日あたり30〜60件
  通常日は0〜5件
  → 全体で 6〜12時間程度（overnight実行推奨）
  ※ 中断しても再開できるのでこまめに実行してもOK
"""

import json
import logging
import os
import sys
import time
import random
from datetime import datetime, date, timedelta
from pathlib import Path

import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from xbrl_parser import parse_disclosure, FinancialSummary

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── 定数 ──────────────────────────────────────────────────────────────────
TDNET_LIST_URL = "https://www.release.tdnet.info/inbs/I_list_{page:03d}_{date}.html"
TDNET_BASE_URL = "https://www.release.tdnet.info/inbs/"

TARGET_KEYWORDS = ["決算短信", "業績予想の修正", "配当予想の修正"]

SLEEP_PAGE      = 1.0   # ページ取得間のsleep（秒）
SLEEP_XBRL      = 1.5   # XBRL ZIP取得間のsleep（秒）
SLEEP_DAY       = 2.0   # 日付またぎのsleep（秒）
SLEEP_ON_429    = 60.0

DEFAULT_DAYS    = 365 * 3   # デフォルト取得日数（3年）

CHECKPOINT_FILE = Path("data/bulk_import_checkpoint.json")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ── 認証 ──────────────────────────────────────────────────────────────────

def get_sheets_client() -> gspread.Client:
    creds_dict = json.loads(os.environ["GOOGLE_SHEETS_CREDS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ── 日付ユーティリティ ─────────────────────────────────────────────────────

def _business_days(start: date, end: date) -> list[date]:
    """start〜end 間の営業日（月〜金）を新しい順で返す"""
    days, d = [], end
    while d >= start:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return days


# ── TDnet 1日分スクレイピング ─────────────────────────────────────────────

def fetch_disclosures_for_date(target_date: date) -> list[dict]:
    """
    指定日のTDnet開示一覧を全ページスクレイピングし、
    決算短信・業績修正・配当修正（訂正除く・XBRL付き）のみ返す。

    Returns:
        [{"document_id", "code", "company_name", "title", "xbrl_zip_url"}, ...]
    """
    date_str = target_date.strftime("%Y%m%d")
    results  = []
    page     = 1

    while True:
        url = TDNET_LIST_URL.format(page=page, date=date_str)
        try:
            res = requests.get(url, timeout=15)
            if res.status_code == 404:
                break
            if res.status_code == 429:
                logger.warning(f"[{date_str}] 429 Rate limit. {SLEEP_ON_429}秒待機...")
                time.sleep(SLEEP_ON_429)
                continue
            res.raise_for_status()
            res.encoding = "utf-8"
        except requests.RequestException as e:
            logger.warning(f"[{date_str}] p{page} 取得失敗: {e}")
            break

        soup  = BeautifulSoup(res.content, "html.parser")
        table = soup.find("table", id="main-list-table")
        if not table:
            break

        rows      = table.find_all("tr")
        found_any = False

        for tr in rows:
            tds = tr.find_all("td")
            if not tds:
                continue
            rec = {}
            for td in tds:
                cls = td.get("class", [])
                if   "kjCode"  in cls: rec["code"]  = td.get_text(strip=True)[:4]
                elif "kjName"  in cls: rec["name"]  = td.get_text(strip=True)
                elif "kjTitle" in cls:
                    a = td.find("a")
                    rec["title"] = a.get_text(strip=True) if a else ""
                elif "kjXbrl"  in cls:
                    a = td.find("a")
                    if a and a.get("href", "").endswith(".zip"):
                        href = a["href"]
                        rec["xbrl_url"] = TDNET_BASE_URL + href
                        rec["doc_id"]   = href.replace(".zip", "")

            if not rec.get("code") or not rec.get("title"):
                continue
            found_any = True

            if "訂正" in rec.get("title", ""):
                continue
            if not any(kw in rec.get("title", "") for kw in TARGET_KEYWORDS):
                continue
            if not rec.get("xbrl_url"):
                continue

            results.append({
                "document_id"  : rec.get("doc_id", ""),
                "code"         : rec["code"],
                "company_name" : rec.get("name", ""),
                "title"        : rec["title"],
                "xbrl_zip_url" : rec["xbrl_url"],
            })

        if not found_any:
            break
        page += 1
        time.sleep(SLEEP_PAGE)

    return results


# ── Google Sheets 書き込み ──────────────────────────────────────────────

def save_to_sheets(summary: FinancialSummary, ws: gspread.Worksheet) -> None:
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
    """処理済み日付（YYYYMMDD文字列）のセットを返す"""
    if CHECKPOINT_FILE.exists():
        try:
            return set(json.load(open(CHECKPOINT_FILE)))
        except Exception:
            pass
    return set()


def save_checkpoint(done: set[str]) -> None:
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    json.dump(list(done), open(CHECKPOINT_FILE, "w"))


# ── メイン ────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Alpha-Detector 過去財務データ一括投入（日付ループ方式）")
    parser.add_argument("--days",  type=int, default=DEFAULT_DAYS,
                        help=f"取得日数（デフォルト: {DEFAULT_DAYS}日 ≒ 3年）")
    parser.add_argument("--dry-run", action="store_true",
                        help="Sheetsへの書き込みを行わず動作確認のみ")
    parser.add_argument("--reset-checkpoint", action="store_true",
                        help="チェックポイントをリセットして最初から再実行")
    args = parser.parse_args()

    # 環境変数チェック（dry-runは不要）
    if not args.dry_run:
        missing = [e for e in ["GOOGLE_SHEETS_CREDS", "GOOGLE_SHEET_ID"]
                   if not os.environ.get(e)]
        if missing:
            print(f"❌ 環境変数が不足しています: {', '.join(missing)}")
            sys.exit(1)

    if args.reset_checkpoint and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logger.info("チェックポイントをリセットしました")

    done_dates = load_checkpoint()
    logger.info(f"処理済み日付（前回分）: {len(done_dates)}日")

    # Sheets接続（dry-runは不要）
    history_ws = None
    if not args.dry_run:
        logger.info("Google Sheetsに接続中...")
        history_ws = get_sheets_client().open_by_key(
            os.environ["GOOGLE_SHEET_ID"]
        ).worksheet("history")

    # 日付リスト生成
    end_date   = date.today()
    start_date = end_date - timedelta(days=args.days)
    all_days   = _business_days(start_date, end_date)
    pending    = [d for d in all_days if d.strftime("%Y%m%d") not in done_dates]

    logger.info(f"期間: {start_date} 〜 {end_date}  "
                f"({len(all_days)}営業日 / 未処理: {len(pending)}日)")
    if args.dry_run:
        logger.info("★ DRY-RUN モード: Sheetsへの書き込みはしません")

    total_docs = 0
    total_rows = 0

    for i, target_date in enumerate(pending):
        date_str  = target_date.strftime("%Y%m%d")
        docs      = fetch_disclosures_for_date(target_date)
        saved     = 0

        if docs:
            logger.info(f"[{i+1}/{len(pending)}] {date_str}: {len(docs)}件の対象開示")
            for doc in docs:
                summary = parse_disclosure(doc)
                if summary:
                    if not args.dry_run:
                        save_to_sheets(summary, history_ws)
                    saved += 1
                    total_rows += 1
                    if args.dry_run:
                        print(f"  [{doc['code']}] {doc['title'][:40]}"
                              f"  {summary.quarter}Q  OP:{summary.cumulative_op:,.0f}万円"
                              f"  進捗:{summary.progress_rate:.1f}%")
                time.sleep(SLEEP_XBRL)
            logger.info(f"  → {len(docs)}件中 {saved}件保存")
            total_docs += len(docs)
        else:
            logger.debug(f"[{i+1}/{len(pending)}] {date_str}: 対象開示なし")

        done_dates.add(date_str)

        # 10日ごとにチェックポイント保存
        if (i + 1) % 10 == 0:
            save_checkpoint(done_dates)
            logger.info(f"  チェックポイント保存（{i+1}/{len(pending)}日完了）")

        time.sleep(SLEEP_DAY + random.uniform(0, 1.0))

    save_checkpoint(done_dates)

    print("\n" + "=" * 50)
    print(f"{'[DRY-RUN] ' if args.dry_run else ''}✅ 完了！")
    print(f"   処理日数   : {len(pending)}営業日")
    print(f"   対象開示数 : {total_docs}件")
    print(f"   投入行数   : {total_rows}件{'（書き込みなし）' if args.dry_run else ''}")
    print("=" * 50)
    if args.dry_run:
        print("\n問題なければ以下を実行してください:")
        print("  python scripts/bulk_import_history.py")


if __name__ == "__main__":
    main()
