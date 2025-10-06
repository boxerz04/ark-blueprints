# -*- coding: utf-8 -*-
"""
build_tenji_prior_from_raw.py
- data/raw/<*.csv> を preprocess.load_raw で結合
- 指定期間 [--from, --to] を抽出して "展示タイム prior" を作成
- キー: (place, wakuban, season_bin)
- 出力: data/priors/tenji/tenji_prior__<from>_<to>__keys-place-wakuban-seasonbin__sdfloor-<val>__m<m>__v1.csv
"""

from pathlib import Path
import argparse
import pandas as pd
import numpy as np

from preprocess import load_raw, cast_and_clean  # 既存ユーティリティを再利用


def ensure_dir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)


def parse_args():
    ap = argparse.ArgumentParser(description="Build Tenji prior from raw CSVs.")
    ap.add_argument("--raw-dir", type=str, default="data/raw", help="日次CSVが並ぶディレクトリ")
    ap.add_argument("--from", dest="from_date", type=str, required=True, help="開始日 YYYYMMDD")
    ap.add_argument("--to", dest="to_date", type=str, required=True, help="終了日 YYYYMMDD")
    ap.add_argument("--m-strength", type=int, default=200, help="Empirical Bayes の事前強度 m")
    ap.add_argument("--sd-floor", type=float, default=0.02, help="標準偏差の下限（Z発散防止）")
    ap.add_argument("--out", type=str, required=True, help="出力CSVパス（data/priors/tenji/... を推奨）")
    ap.add_argument("--link-latest", action="store_true", help="同フォルダに latest.csv を作成/上書き")
    return ap.parse_args()


def main():
    args = parse_args()
    raw_dir = Path(args.raw_dir)
    out_path = Path(args.out)
    ensure_dir(out_path)

    print(f"[INFO] load_raw: {raw_dir}")
    df_raw = load_raw(raw_dir)

    # 型変換・日付/数値の正規化（preprocess と同じ規則に合わせる）
    df, _report = cast_and_clean(df_raw)

    # 期間絞り込み（inclusive）
    s = pd.to_datetime(args.from_date, format="%Y%m%d")
    e = pd.to_datetime(args.to_date,   format="%Y%m%d")
    mask = (df["date"] >= s) & (df["date"] <= e)
    df = df.loc[mask].copy()
    print(f"[INFO] period: {args.from_date}..{args.to_date} -> rows={len(df)}")

    # 期間にデータが無い場合は、利用可能な date 範囲を案内して終了
    if len(df) == 0:
        if "date" in df_raw.columns and len(df_raw) > 0:
            dmin = pd.to_datetime(df_raw["date"]).min()
            dmax = pd.to_datetime(df_raw["date"]).max()
            raise ValueError(
                f"指定期間に該当する行がありません。利用可能な date 範囲: {dmin:%Y-%m-%d} 〜 {dmax:%Y-%m-%d}"
            )
        raise ValueError("raw データが空、または date 列が見つかりません。")

    # 行ごとに season_bin を付与（10–3: cold, 4–9: warm）
    months = df["date"].dt.month
    df["season_bin"] = np.where(months.isin([10, 11, 12, 1, 2, 3]), "cold", "warm")

    # 必須列チェック
    need = {"time_tenji", "place", "wakuban", "date", "season_bin"}
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise KeyError(f"必要列が見つかりません: {missing}")

    # 展示タイムの有効値のみ
    df = df.dropna(subset=["time_tenji", "place", "wakuban"])
    df = df.loc[df["time_tenji"] > 0].copy()
    if len(df) == 0:
        raise ValueError("指定期間に有効な展示タイム行がありません。")

    # グローバル統計（縮約の基準）
    mu_g = float(df["time_tenji"].mean())
    sd_g = float(df["time_tenji"].std(ddof=0))
    print(f"[INFO] global mu={mu_g:.4f}, sd={sd_g:.4f}")

    # place × wakuban × season_bin で集計
    g = df.groupby(["place", "wakuban", "season_bin"])
    tbl = g.agg(
        tenji_mu=("time_tenji", "mean"),
        tenji_sd=("time_tenji", "std"),
        n_tenji=("time_tenji", "size"),
    ).reset_index()

    # Empirical Bayes 縮約
    m = int(args.m_strength)
    w = tbl["n_tenji"] / (tbl["n_tenji"] + m)
    tbl["tenji_mu"] = w * tbl["tenji_mu"] + (1.0 - w) * mu_g
    tbl["tenji_sd"] = (w * tbl["tenji_sd"].fillna(sd_g) + (1.0 - w) * sd_g).clip(lower=float(args.sd_floor))

    # メタ列（再現性用）
    tbl["built_from"] = args.from_date
    tbl["built_to"] = args.to_date
    tbl["sd_floor"] = float(args.sd_floor)
    tbl["m_strength"] = int(args.m_strength)
    tbl["keys"] = "place-wakuban-seasonbin"
    tbl["version"] = 1

    # 保存
    tbl = tbl.sort_values(["place", "wakuban", "season_bin"]).reset_index(drop=True)
    tbl.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[OK] wrote prior: {out_path} (rows={len(tbl)})")

    # latest.csv の更新
    if args.link_latest:
        latest = out_path.parent / "latest.csv"
        tbl.to_csv(latest, index=False, encoding="utf-8-sig")
        print(f"[OK] updated latest: {latest}")


if __name__ == "__main__":
    main()
