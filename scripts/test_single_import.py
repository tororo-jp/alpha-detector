"""
scripts/test_single_import.py
==============================
1銘柄分の動作確認スクリプト。
Sheetsへの書き込みは行わない。

【使い方（PowerShell）】
  python scripts/test_single_import.py --code 7203
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from bulk_import_history import fetch_xbrl_urls_for_code
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from xbrl_parser import parse_disclosure
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--code", required=True, help="証券コード（例: 7203）")
    args = parser.parse_args()

    print(f"\n[{args.code}] TDnetから開示一覧を取得中...")
    docs = fetch_xbrl_urls_for_code(args.code)

    if not docs:
        print("開示データが見つかりませんでした。")
        return

    print(f"{len(docs)}件の対象開示を発見:\n")
    for i, d in enumerate(docs):
        print(f"  {i+1}. {d['title']}")

    print(f"\n直近3件のXBRLをパースします...\n")
    for doc in docs[:3]:
        summary = parse_disclosure(doc)
        if summary:
            print(f"{'='*50}")
            print(f"タイトル  : {doc['title']}")
            print(f"決算期末  : {summary.fiscal_year_end}")
            print(f"四半期    : {summary.quarter}Q")
            print(f"売上高    : {summary.net_sales:,.0f}万円")
            print(f"営業利益  : {summary.operating_profit:,.0f}万円")
            print(f"純利益    : {summary.net_income:,.0f}万円")
            if summary.forecast_op:
                progress = round(summary.operating_profit / summary.forecast_op * 100, 1)
                print(f"通期予想  : {summary.forecast_op:,.0f}万円（進捗{progress}%）")
            print(f"{'='*50}\n")
        else:
            print(f"  ⚠️ {doc['title']} → パース失敗")
        time.sleep(1.5)

    print("問題なければ以下を実行してください:")
    print("  python scripts/bulk_import_history.py")

if __name__ == "__main__":
    main()
