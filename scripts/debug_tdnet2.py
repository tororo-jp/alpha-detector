"""
TDJFSearchのHTMLテーブル構造を調べる追加デバッグ。
  python scripts/debug_tdnet2.py --code 9433
"""
import argparse, requests
from bs4 import BeautifulSoup

parser = argparse.ArgumentParser()
parser.add_argument("--code", default="9433")
args = parser.parse_args()
code = args.code

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Referer": "https://www.release.tdnet.info/",
}

# ── POST検索の全テーブルを列挙 ──
r = requests.post(
    "https://www.release.tdnet.info/onsf/TDJFSearch/TDJFSearch",
    data={"t0": "20230101", "t1": "20260306", "q": "", "m": code},
    headers=headers, timeout=15,
)
soup = BeautifulSoup(r.content, "html.parser")

print("=== 全テーブルのid/class ===")
for t in soup.find_all("table"):
    print(f"  id={t.get('id')} class={t.get('class')}")

print("\n=== 全テーブルの最初の行 ===")
for t in soup.find_all("table"):
    rows = t.find_all("tr")
    if rows:
        cells = [td.get_text(strip=True)[:30] for td in rows[0].find_all(["td","th"])]
        print(f"  [{t.get('id')}] {cells}")

print("\n=== body全文（先頭2000文字）===")
print(r.text[:2000])
