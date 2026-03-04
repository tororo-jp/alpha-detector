"""
scripts/test_single_import.py
==============================
bulk_import_history.py を実行する前に、
1銘柄分の動作確認をするためのスクリプト。

【使い方】
  JQUANTS_API_KEY='...' python scripts/test_single_import.py --code 7203

J-Quantsから取得した生データと、変換後のhistory行を
ターミナルに表示するのみ（Sheetsへの書き込みは行わない）。
"""

import argparse
import os
import sys

sys.path.insert(0, "scripts")
from bulk_import_history import (
    _auth_headers,
    fetch_fins_for_code,
    statements_to_history_rows,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--code", required=True, help="確認する証券コード（例: 1234）")
    args = parser.parse_args()

    if not os.environ.get("JQUANTS_API_KEY"):
        print("❌ JQUANTS_API_KEY を環境変数に設定してください")
        sys.exit(1)

    print(f"[{args.code}] J-Quants V2 APIから財務データを取得中...")
    statements = fetch_fins_for_code(args.code)

    print(f"\n取得件数: {len(statements)}件の開示")
    if not statements:
        print("データが取得できませんでした。銘柄コードを確認してください。")
        return

    print("\n--- 変換後のhistoryシート用データ ---")
    rows = statements_to_history_rows(args.code, statements)
    rows.sort(key=lambda r: (r["fiscal_year"], r["quarter"]))

    print(f"{'年度':<6} {'Q':<3} {'売上(万円)':<12} {'営業利益(万円)':<14} {'純利益(万円)':<12} {'進捗率':<8}")
    print("-" * 60)
    for r in rows:
        print(
            f"{r['fiscal_year']:<6} "
            f"{r['quarter']}Q   "
            f"{r['cumulative_sales']:>10,.0f}  "
            f"{r['cumulative_op']:>12,.0f}  "
            f"{r['cumulative_net']:>10,.0f}  "
            f"{r['progress_rate']:>6.1f}%"
        )

    print(f"\n合計 {len(rows)}件がhistoryシートに投入される予定です。")
    print("問題なければ bulk_import_history.py を実行してください。")


if __name__ == "__main__":
    main()
