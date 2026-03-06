"""
scripts/debug_tdnet.py
======================
TDnetのURLパターンを調査するデバッグスクリプト。
問題の切り分けに使用してください。

【使い方（PowerShell）】
  python scripts/debug_tdnet.py --code 9433
"""

import argparse
import sys
import requests
from bs4 import BeautifulSoup

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--code", default="9433")
    args = parser.parse_args()
    code = args.code

    print(f"\n{'='*60}")
    print(f"TDnet URLデバッグ: 銘柄コード {code}")
    print(f"{'='*60}\n")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ja,en;q=0.9",
        "Referer": "https://www.release.tdnet.info/",
    }

    tests = [
        # パターン1: paramsで渡す
        ("パターン1 (params方式)",
         "GET",
         "https://www.release.tdnet.info/inbs/I_list_00.html",
         {"Sccode": code, "Sort": "1", "page": "1"},
         None),

        # パターン2: クエリを直接URLに書く
        ("パターン2 (URL直書き)",
         "GET",
         f"https://www.release.tdnet.info/inbs/I_list_00.html?Sccode={code}&Sort=1&page=1",
         None,
         None),

        # パターン3: pageなし
        ("パターン3 (pageパラメータなし)",
         "GET",
         f"https://www.release.tdnet.info/inbs/I_list_00.html?Sccode={code}&Sort=1",
         None,
         None),

        # パターン4: 日付で検索（当日の一覧）
        ("パターン4 (日付指定一覧 I_list_001_YYYYMMDD.html)",
         "GET",
         "https://www.release.tdnet.info/inbs/I_list_001_20260304.html",
         None,
         None),

        # パターン5: TDJFSearch POSTで銘柄コード検索
        ("パターン5 (TDJFSearch POST)",
         "POST",
         "https://www.release.tdnet.info/onsf/TDJFSearch/TDJFSearch",
         None,
         {"t0": "20230101", "t1": "20260306", "q": "", "m": code}),
    ]

    session = requests.Session()
    # まずトップページにアクセスしてCookieを取得
    try:
        session.get("https://www.release.tdnet.info/inbs/I_main_00.html",
                    headers=headers, timeout=10)
        print("✅ TDnetトップページへのアクセス成功（Cookie取得済み）\n")
    except Exception as e:
        print(f"⚠️ トップページアクセス失敗: {e}\n")

    for name, method, url, params, data in tests:
        print(f"── {name}")
        print(f"   URL: {url}")
        if params:
            print(f"   params: {params}")
        if data:
            print(f"   data: {data}")
        try:
            if method == "GET":
                r = session.get(url, params=params, headers=headers, timeout=15)
            else:
                r = session.post(url, data=data, headers=headers, timeout=15)

            print(f"   → ステータス: {r.status_code}")
            print(f"   → 最終URL: {r.url}")
            print(f"   → Content-Type: {r.headers.get('Content-Type', '不明')}")
            print(f"   → レスポンス先頭300文字:")
            print(f"      {r.text[:300].replace(chr(10), ' ')}")

            # HTMLの場合はテーブルの存在を確認
            if "html" in r.headers.get("Content-Type", "").lower():
                soup = BeautifulSoup(r.content, "html.parser")
                table = soup.find("table", id="main-list-table")
                trs   = table.find_all("tr") if table else []
                print(f"   → main-list-table: {'あり' if table else 'なし'}")
                print(f"   → 行数: {len(trs)}")
                if trs:
                    # 最初の行を表示
                    first = trs[0]
                    codes = [td.get_text(strip=True) for td in first.find_all("td")]
                    print(f"   → 1行目: {codes}")

        except Exception as e:
            print(f"   → ❌ エラー: {e}")
        print()

    print("デバッグ完了。上記の結果を共有してください。")

if __name__ == "__main__":
    main()
