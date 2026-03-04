"""
scripts/test_single_import.py
==============================
bulk_import_history.py を実行する前に、
直近7日間のデータで動作確認するスクリプト。
Sheetsへの書き込みは行わない。

【使い方（PowerShell）】
  $env:JQUANTS_API_KEY = "your-api-key"
  python scripts/test_single_import.py

  # 取得日数を変更する場合
  python scripts/test_single_import.py --days 14
"""

import argparse
import os
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, "scripts")
from bulk_import_history import (
    _business_days,
    fetch_summary_by_date,
    summaries_to_history_rows,
    MARKET_CODES,
    SLEEP_BETWEEN_REQUESTS,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="確認する日数（デフォルト: 7日）")
    parser.add_argument(
        "--market", choices=["growth", "standard", "both"], default="both"
    )
    args = parser.parse_args()

    if not os.environ.get("JQUANTS_API_KEY"):
        print("❌ JQUANTS_API_KEY を環境変数に設定してください")
        sys.exit(1)

    end_date   = date.today()
    start_date = end_date - timedelta(days=args.days)
    days       = _business_days(start_date, end_date)
    target_codes = MARKET_CODES[args.market]

    print(f"直近{args.days}日間（{len(days)}営業日）のデータを確認中...\n")

    total_records = 0
    total_rows    = 0

    for target_date in days:
        records = fetch_summary_by_date(target_date)
        rows    = summaries_to_history_rows(records, target_codes)
        total_records += len(records)
        total_rows    += len(rows)

        if rows:
            print(f"【{target_date}】{len(records)}件取得 → {len(rows)}件変換")
            print(f"  {'コード':<6} {'年度':<6} {'Q':<3} {'売上(万円)':>12} {'営業利益(万円)':>14} {'進捗率':>7}")
            print(f"  {'-'*55}")
            for r in rows[:5]:  # 最大5件だけ表示
                print(
                    f"  {r['code']:<6} {r['fiscal_year']:<6} {r['quarter']}Q  "
                    f"{r['cumulative_sales']:>12,.0f} "
                    f"{r['cumulative_op']:>14,.0f} "
                    f"{r['progress_rate']:>6.1f}%"
                )
            if len(rows) > 5:
                print(f"  ... 他{len(rows)-5}件")
        else:
            print(f"【{target_date}】データなし（休場日等）")

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    print(f"\n合計: {total_records}件取得 → {total_rows}件がhistoryシートに投入されます")
    print("\n問題なければ以下を実行してください:")
    print("  python scripts/bulk_import_history.py")


if __name__ == "__main__":
    main()
