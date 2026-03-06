# 🔍 Alpha-Detector

> **決算サプライズ検知システム** — 中小型株の「異常な好決算」をリアルタイムで自動検知・Discord通知する

**完全無料・J-Quants不要・TDnetスクレイピング方式**

---

## 📖 目次

1. [何をするシステムか](#-何をするシステムか)
2. [システム構成](#-システム構成)
3. [スコアリングロジック](#-スコアリングロジック)
4. [フィルター一覧](#-フィルター一覧)
5. [通知サンプル](#-通知サンプル)
6. [セットアップ手順](#️-セットアップ手順)
7. [初期データ投入](#-初期データ投入)
8. [GitHubへの反映方法](#-githubへの反映方法)
9. [ファイル構成](#-ファイル構成)
10. [費用・制約](#-費用制約)
11. [トラブルシューティング](#-トラブルシューティング)

---

## 🎯 何をするシステムか

```
① 毎営業日 15:00〜16:05 に GitHub Actions が自動起動（5分間隔）
        ↓
② TDnet（東証適時開示）をスクレイピングして決算発表を検知
        ↓
③ 発表企業のXBRL（決算短信データ）をダウンロードして財務数値を自動抽出
        ↓
④ 過去3年の同四半期実績と比較してスコアリング（100点満点）
        ↓
⑤ 80点以上（S評価）または60点以上（A評価）の銘柄をDiscordに即時通知
```

対象: **東証グロース・スタンダード上場** / **時価総額1,000億円以下** の中小型株

---

## 🏗 システム構成

```
GitHub Actions（スケジューラー・無料）
    │
    ├─ TDnet適時開示 HTML ─────→ tdnet_watcher.py  ← 新規開示を検知
    │  release.tdnet.info              │                （無料・リアルタイム）
    │                                  ↓
    ├─ TDnet XBRL ZIP ──────────→ xbrl_parser.py   ← 財務数値を抽出
    │  （決算短信の構造化データ）        │                （JP GAAP/IFRS/US GAAP対応）
    │                                  ↓
    ├─ yfinance ────────────────→ price_analyzer.py ← 株価・対TOPIX取得
    │                                  │
    ├─ JPX公式PDF ──────────────→ shinyo_fetcher.py ← 信用残取得
    │                                  │
    │                                  ↓
    ├─ Google Sheets ───────────→ history_db.py     ← 過去3年履歴DB
    │  （財務履歴・信用残・処理済みID）  │
    │                                  ↓
    │                          scoring_engine.py    ← スコアリング
    │                                  │
    └─ Discord Webhook ─────────← notifier.py       ← S/A評価を通知
```

### 使用サービスと費用

| サービス | 用途 | 費用 |
|---------|------|------|
| **GitHub Actions** | スケジューラー・実行環境 | 無料（パブリックRepo） |
| **TDnet 適時開示** | リアルタイム開示検知・XBRL取得 | **無料**（公開HTML/ZIP） |
| **yfinance** | 株価・出来高データ取得 | 無料 |
| **Google Sheets** | 財務履歴・信用残DB | 無料 |
| **JPX公式PDF** | 銘柄別信用取引週末残高 | 無料 |
| **Discord Webhook** | 検知結果の通知 | 無料 |

> **合計費用: 月0円** （J-Quants不要・すべて無料）

---

## 📊 スコアリングロジック

総合スコア **100点満点**。**80点以上 → S評価（即時通知）**、60点以上 → A評価（通知）

### ① 季節性補正済み進捗スコア（最大40点）

```
進捗率乖離 = 今回の進捗率 − 過去3年の同Q平均進捗率

例: 今回2Q進捗率 55% / 過去3年平均 44% → 乖離 +11%
  → 40点 × (11% / 10%) = 44点 → 上限40点
```

> **なぜ「過去3年平均との差」で見るのか？**
> 季節性の強い業種（小売・観光など）は毎年同じ時期に進捗が高くなる。
> 「今年の進捗率が高い」ではなく「例年より高い」かどうかが重要。

### ② モメンタム加速スコア（最大30点）

```
単Q営業利益率改善 = 当Q単Q営業利益率 − 前年同Q単Q営業利益率

例: 当2Q単Q利益率 8.5% / 前年2Q単Q 6.0% → 改善 +2.5pt
  → 30点 × (2.5pt / 2.0pt) = 37.5点 → 上限30点
```

> 単Q = 累計から前Q累計を差し引いた「その四半期だけ」の値。
> 累計では見えない「最新四半期の収益加速」を捉える。

### ③ 上方修正・増配フラグ（30点）

```
上方修正（業績予想の修正）または増配（配当予想の修正）があれば +30点
```

### スコア計算例

| 項目 | 計算 | 得点 |
|------|------|------|
| 進捗率乖離 +11% | 40 × 11/10 → 上限 | 40点 |
| 単Q利益率改善 +1.5pt | 30 × 1.5/2.0 | 22点 |
| 上方修正あり | フラグ | 30点 |
| **合計** | | **92点 → S評価** |

---

## 🚨 フィルター一覧

スコアによらず、以下の条件に該当する場合は警告を通知に付与します。

| フィルター | 条件 | 目的 |
|-----------|------|------|
| **期待値織り込み** | 直近20日で対TOPIX+15%超 | 既に株価に織り込まれた銘柄を除外 |
| **利益の質** | 純利益÷営業利益 > 1.5 | 特別利益・資産売却による見かけ上の好決算を除外 |
| **需給悪化** | 信用倍率 > 10倍 | 売り圧力が強い銘柄を警告 |
| **営業赤字** | 営業利益 ≤ 0 | 収益性に問題がある銘柄を警告 |
| **履歴不足** | 過去3年の同Q実績が3件未満 | 比較対象がない銘柄を除外（データ蓄積後に自動解除） |

---

## 📨 通知サンプル

```
🔥【92点:S評価】[1234] 〇〇テクノロジー株式会社

### 📈 業績サマリー（2Q累計）
- 進捗率: **55.0%**（過去3年平均:44.0%  乖離:**+11.0%**）
- 単Q営業利益率: 8.5%（前年同期:6.0%  変化:+2.5pt）
- イベント: 上方修正 ✅

### ⚠️ 需給・織り込みチェック
- 株価: 1,250円  直近20日対TOPIX:+3.2%
- 信用残: 信用倍率2.1倍（買:120,000株 / 売:57,000株）

### 🔍 フィルター
なし

### 📊 スコア内訳（92/100点）
進捗:40/40  モメンタム:22/30  修正/増配:30/30
```

---

## ⚙️ セットアップ手順

### 必要なもの

- [ ] GitHubアカウント
- [ ] Googleアカウント（Google Sheetsに使用）
- [ ] Discordサーバー（通知受け取り用）

---

### Step 1: Google Sheets APIのセットアップ

#### 1-1. GCPプロジェクトの作成

1. [Google Cloud Console](https://console.cloud.google.com/) にアクセス
2. 「プロジェクトを作成」→ プロジェクト名: `alpha-detector`
3. 作成したプロジェクトを選択

#### 1-2. APIの有効化

1. 左メニュー「APIとサービス」→「ライブラリ」
2. 「Google Sheets API」を検索 → 有効化
3. 「Google Drive API」を検索 → 有効化

#### 1-3. サービスアカウントの作成

1. 「APIとサービス」→「認証情報」→「認証情報を作成」→「サービスアカウント」
2. サービスアカウント名: `alpha-detector-sa` → 作成
3. 作成されたサービスアカウントをクリック → 「キー」タブ
4. 「鍵を追加」→「新しい鍵を作成」→「JSON」→ ダウンロード
5. ダウンロードした `service-account-key.json` を安全な場所に保存

#### 1-4. Google Sheetsの作成とシート初期化

1. [Google Sheets](https://sheets.google.com) で新規スプレッドシートを作成
2. スプレッドシートのURLから `GOOGLE_SHEET_ID` をコピー
   ```
   https://docs.google.com/spreadsheets/d/【ここがSHEET_ID】/edit
   ```
3. スプレッドシートの「共有」ボタン → JSONキー内の `client_email` を追加（編集者権限）

> **`client_email` の確認方法**
> JSONキーファイルを開いて `"client_email"` フィールドの値をコピーしてください。
> 例: `alpha-detector-sa@your-project-id.iam.gserviceaccount.com`

4. PowerShellでシートを初期化:

```powershell
$env:GOOGLE_SHEETS_CREDS = Get-Content "C:\path\to\service-account-key.json" -Raw `
  | ForEach-Object { $_ -replace "`r`n|`n", "" }
$env:GOOGLE_SHEET_ID = "your-sheet-id-here"

python scripts/setup_sheets.py
```

---

### Step 2: Discord Webhookの作成

1. Discordサーバーの通知を受け取りたいチャンネルを右クリック
2. 「チャンネルを編集」→「連携サービス」→「ウェブフック」→「新しいウェブフック」
3. 作成したWebhookの「ウェブフックURLをコピー」

---

### Step 3: GitHubリポジトリの作成

1. [GitHub](https://github.com) で新規リポジトリを作成（名前: `alpha-detector`）
2. Publicに設定（GitHub Actionsを無料で使うため）

---

### Step 4: GitHub Secretsの登録

リポジトリの「Settings」→「Secrets and variables」→「Actions」→「New repository secret」

| Secret名 | 値 |
|---------|---|
| `DISCORD_WEBHOOK_URL` | DiscordのWebhook URL |
| `GOOGLE_SHEETS_CREDS` | JSONキーの**1行圧縮版**（下記参照） |
| `GOOGLE_SHEET_ID` | スプレッドシートのID |

#### JSONキーを1行に圧縮する方法（PowerShell）

```powershell
# service-account-key.jsonのパスを指定してクリップボードにコピー
(Get-Content "C:\path\to\service-account-key.json" -Raw) `
  -replace "`r`n|`n", "" | Set-Clipboard

# クリップボードの内容をGitHub Secretsに貼り付け
```

---

### Step 5: 動作確認

コードをGitHubにプッシュした後（[GitHubへの反映方法](#-githubへの反映方法)を参照）:

1. GitHubリポジトリ → 「Actions」タブ
2. 左サイドバー「Alpha-Detector (Market Hours)」を選択
3. 「Run workflow」ボタン → 「Run workflow」をクリック
4. 実行中の行をクリック → 「detect」ジョブ → 「Run Alpha-Detector」ステップを展開

**正常時のログ**
```
2026-03-05 06:05:01 INFO === Alpha-Detector 起動 (TDnetスクレイピング方式) ===
2026-03-05 06:05:01 INFO 処理済みID: 0件
2026-03-05 06:05:03 INFO TDnet取得完了: 0件の新規対象開示
2026-03-05 06:05:03 INFO 新規開示なし → 終了
```

> 平日の15:00〜16:05以外に手動実行した場合は「新規開示なし → 終了」が正常です。

---

## 📥 初期データ投入

過去3年分の財務履歴がないと全銘柄がスキップされます。
**J-Quants不要・完全無料**でTDnetから過去5年分を取得できます。

### 仕組み

TDnetの銘柄コード指定URLは存在しないため、**日付指定URL**（リアルタイム検知と同じURL）を過去3年分ループします。

```
I_list_001_YYYYMMDD.html（1日1リクエスト）
  → その日に発表された全銘柄の開示をまとめて取得
  → 決算短信・業績修正・配当修正のみフィルタ
  → XBRL ZIPをダウンロード・パース（リアルタイム検知と同一処理）
  → Google Sheetsのhistoryシートに追記
```

### 実行手順（PowerShell）

#### ① 事前準備

```powershell
# 依存ライブラリのインストール
pip install requests beautifulsoup4 lxml gspread google-auth yfinance pandas openpyxl xlrd
```

#### ② JSONキーを1行に圧縮してクリップボードにコピー

```powershell
(Get-Content "C:\path\to\service-account-key.json" -Raw) `
  -replace "`r`n|`n", "" | Set-Clipboard
```

#### ③ 直近7日間でテスト（Sheetsへの書き込みなし）

```powershell
python scripts/test_single_import.py
```

正常なら以下のように表示されます:
```
直近7日間（5営業日）の開示を確認します...

【2026-03-04】3件の対象開示
  ✅ [2590] 2026年1月期 決算短信〔日本基準〕（連結）
       2Q  売上:120,000万  営業利益:8,500万  進捗:52.3%
  ✅ [3816] ...
```

#### ④ 動作確認（dry-runモード・Sheetsへの書き込みなし）

```powershell
$env:GOOGLE_SHEET_ID    = "your-sheet-id"
$env:GOOGLE_SHEETS_CREDS = Get-Clipboard

python scripts/bulk_import_history.py --dry-run --days 30
```

#### ⑤ 全件投入（過去3年分 / 6〜12時間）

```powershell
python scripts/bulk_import_history.py
```

**中断後の再開**（そのままもう一度実行するだけ）
```powershell
python scripts/bulk_import_history.py
```

**最初からやり直す場合のみ**
```powershell
python scripts/bulk_import_history.py --reset-checkpoint
```

> ⏱ **処理時間の目安**
> 3年分 ≒ 780営業日。決算集中月（2・3・5・8・11月）は1日あたり30〜60件のXBRL取得が発生。
> 全体で **6〜12時間程度**（overnight実行推奨）。
>
> 10日ごとにチェックポイントが保存されるため、途中中断しても再開できます。

---

## 🚀 GitHubへの反映方法

### 初回：リポジトリへのアップロード

```powershell
# 1. Gitが入っているか確認
git --version

# 2. プロジェクトフォルダに移動（alpha-detectorフォルダの中）
cd alpha-detector

# 3. Gitを初期化
git init

# 4. GitHubのリポジトリをリモートに登録
git remote add origin https://github.com/<あなたのユーザー名>/alpha-detector.git

# 5. 全ファイルをステージング
git add .

# 6. コミット
git commit -m "initial commit"

# 7. GitHubにプッシュ
git push -u origin main
```

> ⚠️ `git push` で認証が求められた場合は、GitHubの「Settings → Developer settings → Personal access tokens」でトークンを発行して使用してください。

### ファイルを修正した後の反映

```powershell
# 変更内容を確認
git status

# 全変更をステージング
git add .

# コミット（変更内容を日本語で書いてもOK）
git commit -m "TDnetスクレイピング方式に変更"

# GitHubに反映
git push
```

### よく使うGitコマンド

| コマンド | 意味 |
|---------|------|
| `git status` | 変更されたファイルの一覧を確認 |
| `git add .` | 全変更をステージング |
| `git add src/main.py` | 特定ファイルだけをステージング |
| `git commit -m "メッセージ"` | 変更を記録 |
| `git push` | GitHubに反映 |
| `git pull` | GitHubから最新を取得 |
| `git log --oneline` | コミット履歴を確認 |

### 手動でActionsを動かす方法

```
GitHubリポジトリ
  → 「Actions」タブ
  → 左サイドバー「Alpha-Detector (Market Hours)」
  → 「Run workflow」ボタン
  → 「Run workflow」をクリック
```

---

## 📁 ファイル構成

```
alpha-detector/
│
├── .github/workflows/
│   ├── market_hours.yml        # 平日 15:00〜16:05 / 5分間隔
│   └── weekly_shinyo.yml       # 毎週火曜 17:30（信用残更新）
│
├── src/
│   ├── main.py                 # エントリーポイント
│   ├── tdnet_watcher.py        # TDnet HTMLスクレイピング（開示検知）
│   ├── xbrl_parser.py          # XBRL ZIP解析（財務数値抽出）
│   ├── history_db.py           # Google Sheets 財務履歴DB
│   ├── scoring_engine.py       # スコアリング・フィルタリング
│   ├── price_analyzer.py       # 株価・対TOPIX取得（yfinance）
│   ├── shinyo_fetcher.py       # 信用残取得（JPX PDF）
│   └── notifier.py             # Discord通知
│
├── scripts/
│   ├── bulk_import_history.py  # 過去データ一括投入（TDnet方式）
│   ├── test_single_import.py   # 1銘柄テスト確認
│   └── setup_sheets.py         # Sheetsシート初期化
│
├── data/                       # ローカル一時ファイル（Git管理外）
│   ├── processed_ids.json      # 処理済み開示IDキャッシュ
│   └── bulk_import_checkpoint.json  # 一括投入の進捗
│
├── requirements.txt
└── README.md
```

---

## 💰 費用・制約

### GitHub Actions の使用時間

| 対象 | 頻度 | 1回の時間 | 月間合計 |
|------|------|-----------|---------|
| market_hours.yml | 平日 約26回/日 × 20日 | 約1〜2分 | 約520〜1,040分 |
| weekly_shinyo.yml | 毎週火曜 1回 | 約2〜3分 | 約8〜12分 |

> GitHub Actions の無料枠は **2,000分/月**。上記の使用量は余裕を持って収まります。

### TDnetスクレイピングに関する注意

- TDnetに明示的なスクレイピング禁止規定はありませんが、過剰なアクセスは避けています
- 本システムは5分に1回・1〜3リクエスト程度のアクセスのみで、サーバーへの負荷は最小限です
- XBRL構造はJPXが更新する場合があります。パースエラーが増えた場合は `xbrl_parser.py` の更新が必要になることがあります

### その他

- JPX信用残は**週次（金曜締め・翌火曜16:30公開）**のため当日分はリアルタイムでは反映されません
- 過去3年の履歴データが揃っていない銘柄はスキップされます（初期投入後に自動解除）

---

## 🛠 トラブルシューティング

| 症状 | 原因 | 対処 |
|------|------|------|
| 全銘柄がSKIPになる | historyシートの過去データ不足 | `scripts/bulk_import_history.py` を実行 |
| 「XBRLパース失敗」が多い | TDnetのXBRL構造変更の可能性 | `xbrl_parser.py` のタグ名を確認・更新 |
| Actionsが失敗する（赤×） | 環境変数（Secrets）の設定ミス | GitHub Secrets を再確認 |
| Google Sheetsに書き込めない | サービスアカウントの共有設定漏れ | スプレッドシートの共有にclient_emailを追加 |
| `git push` で認証エラー | GitHubの認証設定 | Personal Access Token を発行して使用 |
| 「新規開示なし → 終了」が続く | 平日15:00〜16:05以外に実行している | スケジュール通り動作しているので正常 |
| test_single_import.py でデータなし | 対象銘柄のXBRL未提供 or 銘柄コードの誤り | 別の銘柄コードで試す（例: 6758, 9984） |

---

## ⚠️ 免責事項

本システムはあくまでも情報収集・スクリーニングのツールです。
投資判断は必ずご自身の責任でお願いします。
本システムの利用によって生じた損失について、開発者は一切の責任を負いません。

