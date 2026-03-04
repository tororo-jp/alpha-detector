"""
tdnet_watcher.py
================
TDnet 適時開示情報閲覧サービスを直接スクレイピングして
当日の新規開示一覧を取得するモジュール。

【データ取得元】
  https://www.release.tdnet.info/inbs/I_list_001_YYYYMMDD.html
  ・開示日を含む31日分が公開されている公式ページ
  ・J-Quants 不要・完全無料・リアルタイム

【取得する情報】
  - 開示時刻 / 証券コード / 会社名 / 表題
  - XBRL ZIPのURL（財務数値の抽出に使用）
"""

import logging
import time
import random
from datetime import datetime

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

TDNET_LIST_URL  = "https://www.release.tdnet.info/inbs/I_list_{page:03d}_{date}.html"
TDNET_BASE_URL  = "https://www.release.tdnet.info/inbs/"

# 対象書類キーワード
TARGET_KEYWORDS = ["決算短信", "業績予想の修正", "配当予想の修正"]
SLEEP_SEC       = 1.5  # ページ間sleep

# 対象取引所（東・名・福・札 → 中小型グロース/スタンダードを含む）
TARGET_EXCHANGES = {"東", "名", "福", "札"}


def _is_target_doc(title: str) -> bool:
    """決算短信・業績/配当修正かどうかを判定"""
    return any(kw in title for kw in TARGET_KEYWORDS)


def _is_amendment(title: str) -> bool:
    """訂正開示かどうかを判定（訂正は除外）"""
    return "訂正" in title


def fetch_new_disclosures(processed_ids: set[str]) -> list[dict]:
    """
    当日のTDnet開示一覧をスクレイピングし、
    未処理かつ対象書類（決算短信・業績修正・配当修正）のみを返す。

    Args:
        processed_ids: 処理済み開示ID（XBRLのZIPファイル名ベース）のセット

    Returns:
        [
          {
            "document_id": str,   # ZIPファイル名（例: 081220240216538699）
            "code": str,          # 証券コード4桁
            "company_name": str,
            "title": str,
            "xbrl_zip_url": str,  # XBRL ZIPのURL
            "disclosed_at": str,  # "HH:MM"
          }, ...
        ]
    """
    today = datetime.now().strftime("%Y%m%d")
    results = []
    page = 1

    while True:
        url = TDNET_LIST_URL.format(page=page, date=today)
        try:
            res = requests.get(url, timeout=15)
            if res.status_code == 404:
                break  # ページ終端
            res.raise_for_status()
            res.encoding = "utf-8"
        except requests.RequestException as e:
            logger.warning(f"TDnet一覧取得失敗 page={page}: {e}")
            break

        soup = BeautifulSoup(res.content, "html.parser")
        table = soup.find("table", id="main-list-table")
        if not table:
            logger.info(f"page={page}: テーブルなし → 終了")
            break

        rows = table.find_all("tr")
        if not rows:
            break

        found_any = False
        for tr in rows:
            tds = tr.find_all("td")
            if not tds:
                continue

            # 各カラムを取得（クラス名で判別）
            rec = {}
            for td in tds:
                classes = td.get("class", [])
                if "kjTime" in classes:
                    rec["time"] = td.get_text(strip=True)
                elif "kjCode" in classes:
                    rec["code"] = td.get_text(strip=True)[:4]
                elif "kjName" in classes:
                    rec["name"] = td.get_text(strip=True)
                elif "kjTitle" in classes:
                    a = td.find("a")
                    rec["title"] = a.get_text(strip=True) if a else ""
                elif "kjXbrl" in classes:
                    a = td.find("a")
                    if a and a.get("href"):
                        href = a.get("href")
                        rec["xbrl_url"] = TDNET_BASE_URL + href
                        # ZIPファイル名（拡張子なし）をIDとして使用
                        rec["doc_id"] = href.replace(".zip", "").replace(".ZIP", "")
                elif "kjPlace" in classes:
                    rec["exchange"] = td.get_text(strip=True).strip()

            # 必須フィールドチェック
            if not rec.get("code") or not rec.get("title"):
                continue

            found_any = True

            # 訂正・対象外書類・処理済み・XBRL無しは除外
            if _is_amendment(rec.get("title", "")):
                continue
            if not _is_target_doc(rec.get("title", "")):
                continue
            if not rec.get("xbrl_url"):
                continue
            if rec.get("doc_id") in processed_ids:
                continue

            results.append({
                "document_id"  : rec.get("doc_id", ""),
                "code"         : rec["code"],
                "company_name" : rec.get("name", ""),
                "title"        : rec["title"],
                "xbrl_zip_url" : rec["xbrl_url"],
                "disclosed_at" : rec.get("time", ""),
            })

        if not found_any:
            break

        page += 1
        time.sleep(SLEEP_SEC + random.uniform(0, 0.5))

    logger.info(f"TDnet取得完了: {len(results)}件の新規対象開示")
    return results
