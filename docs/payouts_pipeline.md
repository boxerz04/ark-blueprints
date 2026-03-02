# Payouts Pipeline

払戻データ用のパイプラインです。払戻ページの HTML を取得し、解析して CSV に集約します。

## 実行コマンド

```bash
python scripts/fetch_payout_html.py --start 20260201 --end 20260202
python scripts/parse_payout_html.py
```

## 生成物

- HTML: `data/html/payouts/payoutsYYYYMMDD.html`
- CSV: `data/processed/payouts/all_payout_results.csv`

## 依存パッケージ

- aiohttp
- beautifulsoup4
