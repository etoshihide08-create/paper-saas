# RehaEvidence / リハエビデンスナビ

## Railway デプロイ

このリポジトリには [railway.json](/Users/toshihide/Documents/myproject/railway.json) を追加してあります。  
Railway の GitHub デプロイ前提で、そのまま使える設定です。

### Railway でやること

1. この変更を GitHub に push する
2. Railway で対象 Project / Service を開く
3. GitHub リポジトリ `paper-saas` を接続する
4. Service に Volume を 1 つ追加する
5. Variables に必要な環境変数を入れる
6. Deploy を実行する

### Railway で追加する Volume

SQLite を永続化するため、Volume を 1 つ追加してください。

- Mount Path: `/data`

このアプリは `DB_NAME` で保存先を切り替えるので、Volume を `/data` に付けたうえで `DB_NAME=/data/papers.db` を設定すれば永続化できます。

### Railway で入力する環境変数

必須:

| 変数名 | 入れる値 | メモ |
| --- | --- | --- |
| `OPENAI_API_KEY` | OpenAI API キー | 論文要約、GEO 記事生成、診断で使用 |
| `ENTREZ_EMAIL` | PubMed / Entrez 用メールアドレス | NCBI 利用時の識別用 |
| `SESSION_SECRET` | ランダムな長い文字列 | ログインセッション用 |
| `DB_NAME` | `/data/papers.db` | SQLite の永続保存先 |

推奨:

| 変数名 | 入れる値 | メモ |
| --- | --- | --- |
| `APP_BASE_URL` | Railway の公開 URL | 記事 CTA・計測リンクに使用 |

任意:

| 変数名 | 用途 | 例 |
| --- | --- | --- |
| `MASTER_ARTICLE_MODEL` | GEO 記事生成モデルの上書き | `gpt-4.1` |
| `MASTER_ARTICLE_REVIEW_MODEL` | GEO 診断モデルの上書き | `gpt-4.1` |
| `WP_SITE_URL` | WordPress 接続の環境変数固定版 | `https://example.com` |
| `WP_USERNAME` | WordPress 接続の環境変数固定版 | `admin` |
| `WP_APP_PASSWORD` | WordPress 接続の環境変数固定版 | アプリパスワード |

補足:

- Railway は `RAILWAY_PUBLIC_DOMAIN` などのシステム変数を提供します
- 公開 URL を生成したあとに `APP_BASE_URL` をその URL に合わせて入れるのが安全です
- `SESSION_SECRET` は Railway 側で自動生成されないので、自分で設定してください
- WordPress 投稿は、環境変数を使わなくても [master_settings.html](/Users/toshihide/Documents/myproject/templates/master_settings.html) から接続設定できます

### Railway 側で確認する値

- Start Command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
- Healthcheck Path: `/`
- Volume Mount Path: `/data`

`railway.json` に start command と healthcheck を入れてあるので、通常は Railway 側で別指定しなくても大丈夫です。

### デプロイ後に最初に確認すること

1. Railway の公開 URL を生成する
2. トップページが開くか確認する
3. 検索結果ページが開くか確認する
4. 新規登録・ログインができるか確認する
5. 論文保存後に再デプロイしてもデータが残るか確認する
6. 公開 URL が決まったら `APP_BASE_URL` を更新する

### Railway の公開 URL が出た後にやること

1. `APP_BASE_URL` をその公開 URL に更新
2. マスター枠アカウントで `/master/settings` を開く
3. 必要なら WordPress 接続設定を保存
4. 自動投稿や CTA 計測を使う場合は、そのまま運用開始

---

## Render デプロイ

このリポジトリには [render.yaml](/Users/toshihide/Documents/myproject/render.yaml) を追加してあります。  
Render の Blueprint として読み込める前提です。

### 先にやること

1. この変更を GitHub に push する
2. Render で `New +` → `Blueprint` を選ぶ
3. GitHub リポジトリ `paper-saas` を接続する
4. ルートの `render.yaml` を読み込ませる
5. 初回作成時に環境変数を入力する

### Render で入力する環境変数

初回 Blueprint 作成時に入力が必要なのは、基本的にこの 3 つです。

| 変数名 | 必須 | 入れる値 | メモ |
| --- | --- | --- | --- |
| `OPENAI_API_KEY` | 必須 | OpenAI API キー | 論文要約、GEO 記事生成、診断で使用 |
| `ENTREZ_EMAIL` | 必須 | PubMed / Entrez 用のメールアドレス | NCBI 利用時の識別用 |
| `APP_BASE_URL` | 推奨 | 例: `https://rehaevidence.onrender.com` | 記事 CTA・計測リンクに使用 |

Render 側で自動設定または `render.yaml` 側で固定されるもの:

| 変数名 | 設定方法 | 値 |
| --- | --- | --- |
| `SESSION_SECRET` | Render 自動生成 | ランダム値 |
| `DB_NAME` | `render.yaml` で固定 | `/var/data/papers.db` |
| `PYTHON_VERSION` | `render.yaml` で固定 | `3.12.8` |

必要になったら後から追加できる任意の環境変数:

| 変数名 | 用途 | 例 |
| --- | --- | --- |
| `MASTER_ARTICLE_MODEL` | GEO 記事生成モデルの上書き | `gpt-4.1` |
| `MASTER_ARTICLE_REVIEW_MODEL` | GEO 診断モデルの上書き | `gpt-4.1` |
| `WP_SITE_URL` | WordPress 接続の環境変数固定版 | `https://example.com` |
| `WP_USERNAME` | WordPress 接続の環境変数固定版 | `admin` |
| `WP_APP_PASSWORD` | WordPress 接続の環境変数固定版 | アプリパスワード |

補足:

- WordPress 投稿は、環境変数を使わなくても [master_settings.html](/Users/toshihide/Documents/myproject/templates/master_settings.html) から接続設定できます
- `APP_BASE_URL` は本番 URL が確定したら正しい URL に更新してください
- Render の `sync: false` な環境変数は、初回 Blueprint 作成時に入力が求められます

### Blueprint 作成時の確認ポイント

- Service 名: `rehaevidence`
- Runtime: `python`
- Plan: `starter`
- Build Command: `pip install -r requirements.txt`
- Start Command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
- Health Check Path: `/`
- Persistent Disk:
  - Name: `rehaevidence-data`
  - Mount Path: `/var/data`
  - Size: `1 GB`

### デプロイ後に最初に確認すること

1. Render の公開 URL でトップページが開くか
2. 検索結果ページが開くか
3. 新規登録・ログインができるか
4. 論文保存ができるか
5. `DB_NAME=/var/data/papers.db` によりデータが保持されるか

### 運用メモ

- SQLite は永続ディスク配下の `/var/data/papers.db` に保存されます
- 永続ディスクを使うため、Render 側では `starter` 以上が前提です
- WordPress 自動投稿を使う場合は、デプロイ後にマスター枠アカウントで `/master/settings` から接続設定してください
- `APP_BASE_URL` を設定すると、WordPress 記事内の CTA と計測リンクが正しく本番 URL を向きます

### 次にやるとスムーズなこと

1. このブランチを GitHub に push
2. Render で Blueprint 作成
3. 初回環境変数を入力
4. デプロイ完了後に `APP_BASE_URL` を最終 URL に合わせて確認
5. マスター枠で WordPress 接続を設定
