"""
tdnet_watcher.py
J-Quants API V2経由でTDnetの適時開示情報を取得するモジュール。

【V2変更点】
  - 認証: リフレッシュトークン方式 → APIキー方式 (x-api-key ヘッダー)
  - エンドポイント: /v1/ → /v2/
  - Secret名: JQUANTS_REFRESH_TOKEN → JQUANTS_API_KEY

対象書類種別:
  - 決算短信（コード: 140系）
  - 業績予想の修正（コード: 160系）
  - 配当予想の修正（コード: 170系）
"""

import os
import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

JQUANTS_DOCS_URL = "https://api.jquants.com/v2/documents/list"

# 決算短信・業績修正・配当修正の書類種別コード
TARGET_TYPE_CODES = {
    "140",  # 決算短信（日本基準）
    "141",  # 決算短信（IFRS）
    "142",  # 決算短信（US-GAAP）
    "160",  # 業績予想の修正
    "170",  # 配当予想の修正
}

# 対象市場（東証グロース・スタンダード）
TARGET_MARKETS = {"グロース", "スタンダード", "Growth", "Standard"}


def _get_api_key() -> str:
    """環境変数からJ-Quants APIキーを取得"""
    api_key = os.environ.get("JQUANTS_API_KEY", "")
    if not api_key:
        raise EnvironmentError("JQUANTS_API_KEY が設定されていません")
    return api_key


def _auth_headers() -> dict:
    """V2認証ヘッダーを返す"""
    return {"x-api-key": _get_api_key()}


def fetch_new_disclosures(processed_ids: set[str]) -> list[dict]:
    """
    当日の適時開示一覧を取得し、未処理かつ対象書類のみ返す。

    Args:
        processed_ids: 処理済み開示IDのセット

    Returns:
        未処理の開示情報リスト。各要素は以下のキーを持つ:
        {
          "document_id": str,
          "code": str,          # 証券コード（4桁）
          "company_name": str,
          "type_code": str,
          "disclosed_at": str,  # ISO形式
          "xbrl_url": str | None,
          "pdf_url": str | None,
        }
    """
    today = datetime.now().strftime("%Y%m%d")

    try:
        res = requests.get(
            JQUANTS_DOCS_URL,
            headers=_auth_headers(),
            params={"date": today},
            timeout=20,
        )
        res.raise_for_status()
        # V2レスポンスは {"data": [...], "pagination_key": "..."} 形式
        body = res.json()
        docs = body.get("data") or body.get("documents", [])
    except requests.RequestException as e:
        logger.error(f"J-Quants API エラー: {e}")
        return []

    results = []
    for d in docs:
        doc_id = str(d.get("document_id", ""))
        type_code = str(d.get("type_code", ""))
        code = str(d.get("local_code", ""))[:4]  # 証券コード4桁

        # フィルタリング
        if doc_id in processed_ids:
            continue
        if type_code not in TARGET_TYPE_CODES:
            continue
        if not _is_target_market(d):
            continue

        results.append({
            "document_id"  : doc_id,
            "code"         : code,
            "company_name" : d.get("company_name", ""),
            "type_code"    : type_code,
            "disclosed_at" : d.get("disclosed_at", ""),
            "xbrl_url"     : d.get("xbrl_url"),
            "pdf_url"      : d.get("pdf_url"),
        })

    logger.info(f"新規対象開示: {len(results)}件")
    return results


def _is_target_market(doc: dict) -> bool:
    """東証グロース・スタンダードのみを対象とする"""
    market = doc.get("market_code", "") or doc.get("market_segment", "")
    # J-Quantsの市場コード: 0109=グロース, 0111=スタンダード（要確認）
    target_codes = {"0109", "0111"}
    if market in target_codes:
        return True
    # 名称でのフォールバック
    for t in TARGET_MARKETS:
        if t in str(market):
            return True
    return False
