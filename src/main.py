"""
main.py
=======
Alpha-Detector メインエントリーポイント。

処理フロー:
  1. 処理済みIDを読み込む
  2. TDnet HTMLをスクレイピングして当日の新規開示一覧を取得
  3. XBRL ZIPをダウンロードして財務数値を抽出
  4. 過去3年の同Q実績をGoogle Sheetsから取得
  5. yfinanceで株価・対TOPIX比を取得
  6. Google Sheetsから信用残を取得
  7. スコアリング・フィルタリング
  8. S/A評価をDiscordに通知
  9. 財務データをGoogle Sheetsに保存
  10. 処理済みIDを保存
"""

import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

from history_db import (
    get_history, get_margin_data,
    load_processed_ids, save_history, save_processed_ids,
)
from notifier import notify_error, notify_result
from price_analyzer import get_price_data
from scoring_engine import run_scoring
from tdnet_watcher import fetch_new_disclosures
from xbrl_parser import parse_disclosure, FinancialSummary


def main() -> None:
    logger.info("=== Alpha-Detector 起動 (TDnetスクレイピング方式) ===")

    processed_ids = load_processed_ids()
    logger.info(f"処理済みID: {len(processed_ids)}件")

    disclosures = fetch_new_disclosures(processed_ids)
    if not disclosures:
        logger.info("新規開示なし → 終了")
        return

    logger.info(f"処理対象: {len(disclosures)}件")
    new_processed: list[str] = []

    for doc in disclosures:
        code   = doc["code"]
        doc_id = doc["document_id"]
        logger.info(f"[{code}] {doc['title']}")

        # XBRL パース
        summary: FinancialSummary | None = parse_disclosure(doc)
        if summary is None:
            logger.warning(f"[{code}] XBRLパース失敗 → スキップ")
            new_processed.append(doc_id)
            continue

        # 過去3年の同Q履歴
        history = get_history(code, summary.quarter)
        if len(history) < 3:
            logger.info(f"[{code}] 履歴不足({len(history)}件) → 保存して次回へ")
            save_history(code, summary)
            new_processed.append(doc_id)
            continue

        # 株価・信用残
        price_data  = get_price_data(code)
        margin_data = get_margin_data(code)

        # スコアリング
        result = run_scoring(summary, history, price_data, margin_data)
        logger.info(
            f"[{code}] {result.total_score}点 ({result.grade}評価)"
            + (f" skip={result.skip_reason}" if result.skip_reason else "")
        )

        # 通知（S/A評価かつスキップなし）
        if result.grade in ("S", "A") and not result.skip_reason:
            notify_result(summary, result, price_data, margin_data)

        save_history(code, summary)
        new_processed.append(doc_id)

    if new_processed:
        save_processed_ids(processed_ids | set(new_processed))
        logger.info(f"処理済みID保存: {len(new_processed)}件追加")

    logger.info("=== Alpha-Detector 終了 ===")


if __name__ == "__main__":
    main()
