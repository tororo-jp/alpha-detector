"""
main.py
Alpha-Detector エントリーポイント。
GitHub Actions から呼び出される。

処理フロー:
  1. 処理済みIDを読み込む
  2. TDnetから新規開示を取得
  3. 各開示についてXBRLをパース
  4. 過去3年履歴チェック（なければスキップ）
  5. 株価・信用残データを取得
  6. スコアリング・フィルタリング
  7. S/A評価をDiscordに通知
  8. 処理済みIDを保存・財務履歴をDBに保存
"""

import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

from history_db import (
    get_history,
    get_margin_data,
    load_processed_ids,
    save_history,
    save_processed_ids,
    QuarterlyResult,
)
from notifier import notify_error, notify_result
from price_analyzer import get_price_data
from scoring_engine import run_scoring
from tdnet_watcher import fetch_new_disclosures
from xbrl_parser import FinancialSummary, parse_disclosure


def main() -> None:
    logger.info("=== Alpha-Detector 起動 ===")

    # ── APIキー存在確認 ──────────────────────────
    if not os.environ.get("JQUANTS_API_KEY"):
        logger.error("JQUANTS_API_KEY が設定されていません")
        notify_error("JQUANTS_API_KEY が設定されていません")
        sys.exit(1)

    # ── 処理済みID読み込み ───────────────────────
    processed_ids = load_processed_ids()
    logger.info(f"処理済みID: {len(processed_ids)}件")

    # ── 新規開示取得 ─────────────────────────────
    new_docs = fetch_new_disclosures(processed_ids)
    if not new_docs:
        logger.info("新規開示なし → 終了")
        return

    logger.info(f"新規開示 {len(new_docs)}件を処理します")
    newly_processed: set[str] = set()

    for doc in new_docs:
        code = doc["code"]
        doc_id = doc["document_id"]
        logger.info(f"[{code}] 処理開始: {doc.get('company_name')} ({doc_id})")

        # ── XBRLパース ───────────────────────────
        summary: FinancialSummary | None = parse_disclosure(doc)
        if summary is None:
            logger.warning(f"[{code}] XBRLパース失敗 → スキップ")
            newly_processed.add(doc_id)
            continue

        # ── 過去3年履歴チェック ───────────────────
        # 同Q・前Q両方の履歴が必要
        history_same_q = get_history(code, summary.quarter)
        if not history_same_q:
            logger.info(f"[{code}] 過去3年履歴なし → 除外（通知なし）")
            newly_processed.add(doc_id)
            # 今回のデータは次年度以降のために保存
            _save_current_result(summary)
            continue

        # 前Q履歴（2Q以降で必要）
        history_prev_q = []
        if summary.quarter > 1:
            history_prev_q = get_history(code, summary.quarter - 1)

        # ── 株価データ取得 ──────────────────────
        price_data = get_price_data(code)
        if price_data is None:
            logger.warning(f"[{code}] 株価取得失敗（処理は継続）")

        # ── 信用残データ取得 ────────────────────
        margin_data = get_margin_data(code)

        # ── スコアリング ────────────────────────
        score = run_scoring(summary, price_data, margin_data)
        logger.info(
            f"[{code}] スコア: {score.total_score}点 ({score.grade}) "
            f"| warnings: {len(score.warnings)}件"
        )

        if score.grade == "SKIP":
            logger.info(f"[{code}] SKIP: {score.skip_reason}")
        else:
            # ── Discord通知 ─────────────────────
            notify_result(score, summary, price_data, margin_data)

        # ── 今回の財務データをDBに保存（次回以降の履歴として使用） ──
        _save_current_result(summary)
        newly_processed.add(doc_id)

    # ── 処理済みID保存 ───────────────────────────
    all_processed = processed_ids | newly_processed
    save_processed_ids(all_processed)
    logger.info(f"=== 完了: {len(newly_processed)}件処理 ===")


def _save_current_result(summary: FinancialSummary) -> None:
    """今回の決算データを履歴DBに保存（次年度以降のための蓄積）"""
    try:
        record = QuarterlyResult(
            code=summary.code,
            fiscal_year=summary.fiscal_year,
            quarter=summary.quarter,
            cumulative_sales=summary.cumulative_sales,
            cumulative_op=summary.cumulative_op,
            cumulative_net=summary.cumulative_net,
            progress_rate=summary.progress_rate,
        )
        save_history(record)
    except Exception as e:
        logger.warning(f"[{summary.code}] 履歴保存失敗（処理は継続）: {e}")


if __name__ == "__main__":
    main()
