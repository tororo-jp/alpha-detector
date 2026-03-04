"""
scripts/setup_sheets.py
初回セットアップ用: Google Sheetsに必要なシートとヘッダーを作成する。

使い方:
  GOOGLE_SHEETS_CREDS='...' GOOGLE_SHEET_ID='...' python scripts/setup_sheets.py
"""

import json
import os
import sys

import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHEETS_CONFIG = {
    "history": [
        "code", "fiscal_year", "quarter",
        "cumulative_sales", "cumulative_op", "cumulative_net",
        "progress_rate", "updated_at",
    ],
    "margin": [
        "code", "buy", "sell", "updated_at",
    ],
    "processed": [
        "doc_id", "saved_at",
    ],
}


def main():
    creds_json = os.environ.get("GOOGLE_SHEETS_CREDS")
    sheet_id   = os.environ.get("GOOGLE_SHEET_ID")

    if not creds_json or not sheet_id:
        print("❌ 環境変数 GOOGLE_SHEETS_CREDS と GOOGLE_SHEET_ID を設定してください")
        sys.exit(1)

    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)

    existing = {ws.title for ws in spreadsheet.worksheets()}

    for sheet_name, headers in SHEETS_CONFIG.items():
        if sheet_name in existing:
            print(f"✅ シート '{sheet_name}' は既に存在します")
        else:
            ws = spreadsheet.add_worksheet(title=sheet_name, rows=10000, cols=len(headers))
            ws.append_row(headers)
            print(f"✅ シート '{sheet_name}' を作成しました")

    print("\nセットアップ完了！")


if __name__ == "__main__":
    main()
