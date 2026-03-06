"""
scripts/test_single_import.py
==============================
直近7日間のデータで動作確認するスクリプト。
Sheetsへの書き込みは行わない。

【使い方（PowerShell）】
  python scripts/test_single_import.py

  # 日数を変更する場合
  python scripts/test_single_import.py --days 14
"""
import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from xbrl_parser import parse_disclosure

sys.path.insert(0, str(Path(__file__).parent))
from bulk_import_history import fetch_disclosures_for_date, _business_days, SLEEP_XBRL


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7,
                        help="確認する過去日数（デフォルト: 7日）")
    args = parser.parse_args()

    end_date   = date.today()
    start_date = end_date - timedelta(days=args.days)
    days       = _business_days(start_date, end_date)

    print(f"\n直近{args.days}日間（{len(days)}営業日）の開示を確認します...\n")

    total_docs = 0
    total_ok   = 0

    for target_date in days:
        docs = fetch_disclosures_for_date(target_date)
        if not docs:
            print(f"【{target_date}】対象開示なし")
            continue

        print(f"【{target_date}】{len(docs)}件の対象開示")
        for doc in docs:
            summary = parse_disclosure(doc)
            total_docs += 1
            if summary:
                total_ok += 1
                print(f"  ✅ [{doc['code']}] {doc['title'][:35]}")
                print(f"       {summary.quarter}Q  売上:{summary.cumulative_sales:,.0f}万  "
                      f"営業利益:{summary.cumulative_op:,.0f}万  進捗:{summary.progress_rate:.1f}%")
            else:
                print(f"  ⚠️  [{doc['code']}] {doc['title'][:35]} → XBRLパース失敗")
            time.sleep(SLEEP_XBRL)

    print(f"\n結果: {total_docs}件中 {total_ok}件パース成功")
    if total_ok > 0:
        print("\n問題なければ以下を実行してください:")
        print("  python scripts/bulk_import_history.py")
    else:
        print("\n⚠️ パース成功が0件です。直近7日間に決算発表がなかった可能性があります。")
        print("  --days 30 で期間を広げて試してみてください:")
        print("  python scripts/test_single_import.py --days 30")


if __name__ == "__main__":
    main()
