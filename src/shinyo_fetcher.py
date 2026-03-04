"""
shinyo_fetcher.py
JPX公式PDFから銘柄別信用取引週末残高を取得してGoogle Sheetsに保存するスクリプト。

実行タイミング: 毎週火曜 17:30 JST（GitHub Actions: weekly_shinyo.yml）

JPX公式URL規則:
  https://www.jpx.co.jp/markets/statistics-equities/margin/
  tvdivq0000001rnl-att/syumatsu{YYYYMMDD}00.pdf
  ※ YYYYMMDD は金曜日の日付
"""

import io
import logging
import os
import sys
from datetime import datetime, timedelta

import pdfplumber
import requests

# srcディレクトリ内で実行されることを想定
sys.path.insert(0, os.path.dirname(__file__))
from history_db import save_margin_batch
from notifier import notify_error

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

JPX_BASE_URL = (
    "https://www.jpx.co.jp/markets/statistics-equities/margin/"
    "tvdivq0000001rnl-att/syumatsu{date}00.pdf"
)
REQUEST_TIMEOUT = 60  # PDFは大きいので長めに設定


def _last_friday(base: datetime | None = None) -> str:
    """直近の金曜日の日付文字列（YYYYMMDD）を返す"""
    d = base or datetime.now()
    days_back = (d.weekday() - 4) % 7  # 4=Friday
    if days_back == 0 and d.weekday() != 4:
        days_back = 7
    return (d - timedelta(days=days_back)).strftime("%Y%m%d")


def fetch_pdf(date_str: str) -> bytes:
    url = JPX_BASE_URL.format(date=date_str)
    logger.info(f"PDFダウンロード中: {url}")
    res = requests.get(url, timeout=REQUEST_TIMEOUT)
    res.raise_for_status()
    return res.content


def parse_pdf(pdf_bytes: bytes) -> dict[str, dict]:
    """
    PDFをパースして銘柄コード → {buy, sell} の辞書を返す。

    PDFカラム構成（JPX標準フォーマット）:
      0: 銘柄コード  1: 銘柄名  2: 市場区分
      3: 信用買い残（株）  4: 前週比  5: 信用売り残（株）  6: 前週比

    ⚠️ JPXのPDFレイアウト変更でカラムがずれる場合があります。
       パース失敗時はDiscordにアラートが飛びます。
    """
    result: dict[str, dict] = {}
    parse_errors = 0

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            table = page.extract_table()
            if not table:
                continue

            for row in table:
                if not row or len(row) < 6:
                    continue

                code_raw = str(row[0]).strip().replace(" ", "")

                # 証券コード：4桁数字のみ
                if not (code_raw.isdigit() and len(code_raw) == 4):
                    continue

                try:
                    buy  = int(str(row[3]).replace(",", "").replace(" ", "").strip())
                    sell = int(str(row[5]).replace(",", "").replace(" ", "").strip())
                    result[code_raw] = {"buy": buy, "sell": sell}
                except (ValueError, IndexError, TypeError):
                    parse_errors += 1
                    if parse_errors <= 5:  # 最初の5件だけログ
                        logger.warning(f"パースエラー (page={page_num}): row={row}")

    if parse_errors > 50:
        msg = (
            f"信用残PDFのパースエラーが{parse_errors}件発生しました。"
            "JPXのPDFフォーマットが変更された可能性があります。"
        )
        logger.error(msg)
        notify_error(msg)

    logger.info(f"パース完了: {len(result)}銘柄 (エラー: {parse_errors}件)")
    return result


def main() -> None:
    date_str = _last_friday()
    logger.info(f"対象日付: {date_str}")

    try:
        pdf_bytes = fetch_pdf(date_str)
    except requests.HTTPError as e:
        msg = f"JPX PDF取得失敗 ({date_str}): {e}"
        logger.error(msg)
        notify_error(msg)
        sys.exit(1)

    margin_data = parse_pdf(pdf_bytes)

    if not margin_data:
        msg = f"JPX PDF ({date_str}) から有効なデータを取得できませんでした"
        logger.error(msg)
        notify_error(msg)
        sys.exit(1)

    save_margin_batch(margin_data)
    logger.info("信用残データの更新完了")


if __name__ == "__main__":
    main()
