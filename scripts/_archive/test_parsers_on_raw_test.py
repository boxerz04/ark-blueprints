# -*- coding: utf-8 -*-
"""
scripts/test_parsers_on_raw_test.py

data/raw/test 配下の *_raw.csv を全結合し、src/st.py と src/rank.py の
パーサを適用して、チェック用のCSVを出力する。

追加（2026-xx）:
- rank_class == "void"（＝'＿'）が含まれる race_id を抽出し、
  「不成立レース」としてレース単位で除外した場合の件数もQC出力する。

出力先:
data/processed/qc_parsers/
  - raw_test__parsed.csv
  - qc_rank_class_counts.csv
  - qc_st_parse_stats.csv
  - suspects_rank_unknown.csv
  - suspects_ST_nan.csv
  - suspects_ST_tenji_nan.csv
  - qc_void_races.csv
  - qc_void_race_summary.csv
  - raw_test__parsed__void_dropped.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _read_csv_utf8_lenient(fp: Path) -> pd.DataFrame:
    """
    生CSVは表記ゆれがあり得るため、まずは dtype=str で素直に読む。
    エンコーディングが揺れる場合も想定し、utf-8 -> cp932 の順で試す。
    """
    try:
        return pd.read_csv(fp, dtype=str, encoding="utf-8", engine="python")
    except UnicodeDecodeError:
        return pd.read_csv(fp, dtype=str, encoding="cp932", engine="python")


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    raw_test_dir = repo_root / "data" / "raw" / "test"
    out_dir = repo_root / "data" / "processed" / "qc_parsers"
    _ensure_dir(out_dir)

    # src を import できるようにする（実行場所に依存しない）
    sys.path.insert(0, str(repo_root))

    from src.st import parse_st  # noqa: E402
    from src.rank import parse_rank  # noqa: E402

    files = sorted(raw_test_dir.glob("*_raw.csv"))
    if not files:
        raise FileNotFoundError(f"[ERROR] no raw csv found: {raw_test_dir}\\*_raw.csv")

    print(f"[INFO] raw_test_dir: {raw_test_dir}")
    print(f"[INFO] files: {len(files)}")

    dfs = []
    for i, fp in enumerate(files, 1):
        print(f"[INFO] reading {i}/{len(files)}: {fp.name}")
        df = _read_csv_utf8_lenient(fp)
        df["__source_file"] = fp.name
        dfs.append(df)

    all_df = pd.concat(dfs, ignore_index=True)
    print(f"[INFO] rows: {len(all_df)} cols: {len(all_df.columns)}")

    # -----------------------------
    # ST parser (column-aware)
    # -----------------------------
    st_cols_present = [c for c in ["ST", "ST_tenji"] if c in all_df.columns]
    for c in st_cols_present:
        raw_col = f"{c}__raw"
        out_col = f"{c}__parsed"
        all_df[raw_col] = all_df[c]

        if c == "ST_tenji":
            all_df[out_col] = all_df[c].apply(lambda x: parse_st(x, is_tenji=True))
        else:
            all_df[out_col] = all_df[c].apply(lambda x: parse_st(x, is_tenji=False))

    # -----------------------------
    # rank parser
    # -----------------------------
    if "rank" in all_df.columns:
        all_df["rank__raw"] = all_df["rank"]
        parsed = all_df["rank"].apply(parse_rank).apply(pd.Series)

        for col in ["rank_code", "rank_num", "rank_class", "is_start", "is_finish"]:
            if col in parsed.columns:
                all_df[col] = parsed[col]
    else:
        print("[WARN] column 'rank' not found. rank parser will be skipped.")

    # -----------------------------
    # QC: rank counts + unknown samples
    # -----------------------------
    if "rank_class" in all_df.columns:
        qc_rank = (
            all_df["rank_class"]
            .value_counts(dropna=False)
            .rename_axis("rank_class")
            .reset_index(name="count")
        )
        qc_rank.to_csv(out_dir / "qc_rank_class_counts.csv", index=False, encoding="utf-8-sig")

        suspects_rank_unknown = all_df[all_df["rank_class"] == "unknown"].copy()
        keep_cols = [
            c
            for c in [
                "__source_file",
                "race_id",
                "date",
                "code",
                "R",
                "wakuban",
                "entry",
                "rank__raw",
                "rank_code",
            ]
            if c in suspects_rank_unknown.columns
        ]
        suspects_rank_unknown = suspects_rank_unknown[keep_cols].head(2000)
        suspects_rank_unknown.to_csv(
            out_dir / "suspects_rank_unknown.csv", index=False, encoding="utf-8-sig"
        )

        # -----------------------------
        # QC: void races (レース不成立)
        # -----------------------------
        if "race_id" in all_df.columns:
            void_mask = all_df["rank_class"] == "void"
            void_race_ids = sorted(set(all_df.loc[void_mask, "race_id"].dropna().astype(str)))

            qc_void_races = pd.DataFrame({"race_id": void_race_ids})
            qc_void_races.to_csv(out_dir / "qc_void_races.csv", index=False, encoding="utf-8-sig")

            # summary
            total_rows = len(all_df)
            total_races = all_df["race_id"].nunique(dropna=True)

            dropped_df = all_df[~all_df["race_id"].isin(void_race_ids)].copy()
            dropped_rows = total_rows - len(dropped_df)
            dropped_races = total_races - dropped_df["race_id"].nunique(dropna=True)

            summary = pd.DataFrame(
                [
                    {
                        "rows_total": int(total_rows),
                        "races_total": int(total_races),
                        "void_races": int(len(void_race_ids)),
                        "rows_dropped_by_void_race": int(dropped_rows),
                        "races_dropped_by_void_race": int(dropped_races),
                        "rows_after_drop": int(len(dropped_df)),
                        "races_after_drop": int(dropped_df["race_id"].nunique(dropna=True)),
                    }
                ]
            )
            summary.to_csv(out_dir / "qc_void_race_summary.csv", index=False, encoding="utf-8-sig")

            # drop後データも出力（確認用）
            dropped_out = out_dir / "raw_test__parsed__void_dropped.csv"
            dropped_df.to_csv(dropped_out, index=False, encoding="utf-8-sig")
        else:
            print("[WARN] column 'race_id' not found. void race QC will be skipped.")

    # -----------------------------
    # QC: ST parse stats + suspects
    # -----------------------------
    st_stats_rows = []
    for c in st_cols_present:
        raw_col = f"{c}__raw"
        out_col = f"{c}__parsed"

        raw_non_empty = all_df[raw_col].fillna("").astype(str).str.strip().ne("")
        parsed_is_nan = all_df[out_col].isna()

        st_stats_rows.append(
            {
                "col": c,
                "rows_total": int(len(all_df)),
                "raw_non_empty_rows": int(raw_non_empty.sum()),
                "parsed_nan_rows": int(parsed_is_nan.sum()),
                "parsed_nan_rate": float(parsed_is_nan.mean()),
                "parsed_nan_among_raw_non_empty_rate": float(
                    (parsed_is_nan & raw_non_empty).sum() / max(int(raw_non_empty.sum()), 1)
                ),
            }
        )

        suspects = all_df[raw_non_empty & parsed_is_nan].copy()
        keep_cols = [
            col
            for col in [
                "__source_file",
                "race_id",
                "date",
                "code",
                "R",
                "wakuban",
                "entry",
                raw_col,
            ]
            if col in suspects.columns
        ]
        suspects = suspects[keep_cols].head(2000)
        suspects.to_csv(out_dir / f"suspects_{c}_nan.csv", index=False, encoding="utf-8-sig")

    if st_stats_rows:
        pd.DataFrame(st_stats_rows).to_csv(out_dir / "qc_st_parse_stats.csv", index=False, encoding="utf-8-sig")

    # -----------------------------
    # FULL output
    # -----------------------------
    out_full = out_dir / "raw_test__parsed.csv"
    print(f"[INFO] writing parsed full csv: {out_full}")
    all_df.to_csv(out_full, index=False, encoding="utf-8-sig")

    print(f"[DONE] outputs written to: {out_dir}")


if __name__ == "__main__":
    main()
