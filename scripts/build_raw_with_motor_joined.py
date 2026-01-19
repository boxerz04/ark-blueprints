# -*- coding: utf-8 -*-
"""
scripts/build_raw_with_motor_joined.py  (FULL REPLACE)

目的:
- raw（日次CSV）全期間を読み込み（dtype=strで安定化）
- 採用列へ絞り込み（64列→25列＋管理/補助列）
- src/st.py の parse_st を用いて ST / ST_tenji をパース（ST_tenjiはfloat64）
- src/rank.py の parse_rank を用いて rank を分類（finish/dns/dnf/dsq/fs/ls/void）
- void（'＿'）を含む race_id はレース単位で全行除外
- motor_section_snapshot__all.csv を (date, code, motor_number) でJOIN
- motor_id_map__all.csv を effective_from/to の範囲で motor_id 付与
- FULL joined CSV（確認用） + Parquet + QC を data/processed/motor 配下へ出力

重要な修正（今回）:
- code を全入力（raw/snapshot/map）で zfill(2) 正規化し、'1' と '01' 混在を根絶する。

出力:
  out_dir/
    raw_with_motor__all.csv
    raw_with_motor__all.parquet
    qc/*.csv
"""

from __future__ import annotations

import argparse
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd


# ----------------------------
# Utils
# ----------------------------
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def read_csv_lenient(fp: Path, dtype=str) -> pd.DataFrame:
    """UTF-8優先。ダメならcp932で読む。"""
    try:
        return pd.read_csv(fp, dtype=dtype, encoding="utf-8", engine="python")
    except UnicodeDecodeError:
        return pd.read_csv(fp, dtype=dtype, encoding="cp932", engine="python")


def normalize_race_id(s: Optional[str]) -> Optional[str]:
    """
    race_id が 2.02504E+11 のように科学表記で混入した場合に補正する。
    それ以外は strip のみ。
    """
    if s is None:
        return None
    t = str(s).strip()
    if t == "" or t.lower() == "nan":
        return None
    if re.fullmatch(r"\d+", t):
        return t
    if re.fullmatch(r"[0-9]+(\.[0-9]+)?[eE]\+[0-9]+", t):
        try:
            d = Decimal(t)
            return str(int(d))
        except (InvalidOperation, ValueError):
            return t
    return t


def to_dt_from_yyyymmdd_str(s: pd.Series) -> pd.Series:
    """YYYYMMDD（または末尾 .0）を datetime64[ns] に。"""
    return pd.to_datetime(
        s.astype(str).str.replace(r"\.0$", "", regex=True),
        format="%Y%m%d",
        errors="coerce",
    )


def to_dt_generic(s: pd.Series) -> pd.Series:
    """汎用日付パース（YYYYMMDDもカバー）。"""
    dt = pd.to_datetime(s, errors="coerce")
    mask = dt.isna() & s.notna()
    if mask.any():
        dt2 = pd.to_datetime(s[mask].astype(str), format="%Y%m%d", errors="coerce")
        dt.loc[mask] = dt2
    return dt


def coerce_int_nullable(s: pd.Series) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    return x.astype("Int64")


def coerce_float(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("float64")


def normalize_code_2d(series: pd.Series) -> pd.Series:
    """
    開催場コードを2桁文字列へ統一（'1' -> '01'）。
    空やNaNはNA扱い。
    """
    s = series.astype("string").str.strip()
    s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    # 数値っぽいもの（"1.0"等）を "1"へ寄せる
    s = s.str.replace(r"\.0$", "", regex=True)
    # 1〜2桁の数字だけ zfill
    mask = s.str.fullmatch(r"\d{1,2}", na=False)
    s.loc[mask] = s.loc[mask].str.zfill(2)
    return s


# ----------------------------
# Motor ID assignment (range based)
# ----------------------------
def assign_motor_id_by_effective_ranges(
    df: pd.DataFrame,
    map_df: pd.DataFrame,
    date_col: str = "date_dt",
    code_col: str = "code",
    motor_number_col: str = "motor_number",
) -> pd.Series:
    """
    df（joined）に対して、map_df の effective_from/to の範囲で motor_id を付与する。
    map_df が (code, motor_number) に複数行（世代）を持つため merge で行が増えるが、
    付与は「元dfの行」を基準に行う。
    """
    n = len(df)
    out = pd.Series(pd.array([pd.NA] * n, dtype="string"), index=df.index)

    left = df[[code_col, motor_number_col, date_col]].copy()
    left["_orig_row"] = np.arange(n, dtype=np.int64)

    merged = left.merge(
        map_df[[code_col, motor_number_col, "effective_from_dt", "effective_to_dt", "motor_id"]],
        on=[code_col, motor_number_col],
        how="left",
        copy=False,
    )

    in_range = (
        merged[date_col].notna()
        & merged["effective_from_dt"].notna()
        & merged["effective_to_dt"].notna()
        & (merged[date_col] >= merged["effective_from_dt"])
        & (merged[date_col] <= merged["effective_to_dt"])
    )

    ok = merged.loc[in_range, ["_orig_row", "motor_id"]].copy()
    if ok.empty:
        return out

    ok.sort_values(["_orig_row"], inplace=True)
    ok = ok.drop_duplicates(subset=["_orig_row"], keep="first")

    rows = ok["_orig_row"].to_numpy()
    vals = ok["motor_id"].astype("string").to_numpy()
    out.iloc[rows] = vals
    return out


# ----------------------------
# CLI
# ----------------------------
def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--raw_dir", required=True)
    p.add_argument("--snapshot_csv", required=True)
    p.add_argument("--map_csv", required=True)
    p.add_argument("--out_dir", required=True)
    p.add_argument("--write_full_csv", type=int, default=1)
    p.add_argument("--sample_n", type=int, default=50000)
    return p


def main() -> None:
    args = build_argparser().parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    raw_dir = repo_root / args.raw_dir
    snapshot_csv = repo_root / args.snapshot_csv
    map_csv = repo_root / args.map_csv
    out_dir = repo_root / args.out_dir
    qc_dir = out_dir / "qc"

    ensure_dir(out_dir)
    ensure_dir(qc_dir)

    # src import
    import sys
    sys.path.insert(0, str(repo_root))
    from src.st import parse_st  # noqa
    from src.rank import parse_rank  # noqa

    print(f"[INFO] raw_dir: {raw_dir}")
    print(f"[INFO] snapshot_csv: {snapshot_csv}")
    print(f"[INFO] map_csv: {map_csv}")
    print(f"[INFO] out_dir: {out_dir}")

    raw_files = sorted(raw_dir.glob("*_raw.csv"))
    if not raw_files:
        raise FileNotFoundError(f"[ERROR] no raw files found: {raw_dir}\\*_raw.csv")
    print(f"[INFO] raw files: {len(raw_files)}")

    # 採用列（あなたの確定に基づく）
    wanted_cols = [
        # 管理
        "race_id", "date", "code", "R", "section_id",
        # result
        "rank", "wakuban", "entry", "ST", "ST_rank", "winning_trick",
        # beforeinfo
        "entry_tenji", "ST_tenji", "time_tenji", "Tilt", "parts_exchange",
        "temperature", "weather", "wind_speed", "wind_direction",
        # racelist（一部）
        "player_id", "motor_number", "motor_2rentai_rate", "motor_3rentai_rate",
        # チェック用
        "player",
    ]

    dfs: List[pd.DataFrame] = []
    for i, fp in enumerate(raw_files, 1):
        if i == 1 or i % 50 == 0 or i == len(raw_files):
            print(f"[INFO] reading raw: {i}/{len(raw_files)} {fp.name}")

        df = read_csv_lenient(fp, dtype=str)
        df["__source_file"] = fp.name

        cols_present = [c for c in wanted_cols if c in df.columns]
        missing = [c for c in wanted_cols if c not in df.columns]
        if i == 1 and missing:
            print(f"[WARN] missing columns in first file (ignored): {missing}")

        df = df[cols_present + ["__source_file"]].copy()
        dfs.append(df)

    raw_all = pd.concat(dfs, ignore_index=True)
    print(f"[INFO] raw rows: {len(raw_all)}")

    # ---- Key normalization ----
    if "race_id" in raw_all.columns:
        raw_all["race_id"] = raw_all["race_id"].apply(normalize_race_id).astype("string")

    # date / code
    if "date" in raw_all.columns:
        raw_all["date_dt"] = to_dt_from_yyyymmdd_str(raw_all["date"])
    else:
        raw_all["date_dt"] = pd.NaT

    if "code" in raw_all.columns:
        raw_all["code"] = normalize_code_2d(raw_all["code"])
    else:
        raw_all["code"] = pd.NA

    # numeric keys
    if "motor_number" in raw_all.columns:
        raw_all["motor_number"] = coerce_int_nullable(raw_all["motor_number"])

    # ---- Parse ST / ST_tenji ----
    if "ST" in raw_all.columns:
        raw_all["ST__raw"] = raw_all["ST"].astype("string")
        raw_all["ST"] = raw_all["ST"].apply(lambda x: parse_st(x, is_tenji=False)).astype("float64")

    if "ST_tenji" in raw_all.columns:
        raw_all["ST_tenji__raw"] = raw_all["ST_tenji"].astype("string")
        raw_all["ST_tenji"] = raw_all["ST_tenji"].apply(lambda x: parse_st(x, is_tenji=True)).astype("float64")

    # ---- Coerce numerics (safe) ----
    for c in ["time_tenji", "Tilt", "temperature", "wind_speed", "entry_tenji",
              "motor_2rentai_rate", "motor_3rentai_rate"]:
        if c in raw_all.columns:
            raw_all[c] = coerce_float(raw_all[c])

    for c in ["ST_rank", "entry", "wakuban", "R"]:
        if c in raw_all.columns:
            raw_all[c] = coerce_int_nullable(raw_all[c])

    # ---- Parse rank ----
    if "rank" in raw_all.columns:
        raw_all["rank__raw"] = raw_all["rank"].astype("string")
        parsed = raw_all["rank"].apply(parse_rank).apply(pd.Series)

        for col in ["rank_code", "rank_num", "rank_class", "is_start", "is_finish"]:
            if col in parsed.columns:
                raw_all[col] = parsed[col]

        raw_all["rank_code"] = raw_all.get("rank_code", pd.Series([pd.NA] * len(raw_all))).astype("string")
        raw_all["rank_class"] = raw_all.get("rank_class", pd.Series([pd.NA] * len(raw_all))).astype("string")
        raw_all["rank_num"] = pd.to_numeric(raw_all.get("rank_num"), errors="coerce").astype("float64")
        if "is_start" in raw_all.columns:
            raw_all["is_start"] = raw_all["is_start"].astype("boolean")
        if "is_finish" in raw_all.columns:
            raw_all["is_finish"] = raw_all["is_finish"].astype("boolean")

    # ---- VOID race exclusion ----
    void_race_ids: List[str] = []
    if "race_id" in raw_all.columns and "rank_class" in raw_all.columns:
        void_mask = raw_all["rank_class"] == "void"
        void_race_ids = sorted(set(raw_all.loc[void_mask, "race_id"].dropna().astype(str)))

        before_rows = len(raw_all)
        before_races = raw_all["race_id"].nunique(dropna=True)

        if void_race_ids:
            raw_all = raw_all[~raw_all["race_id"].isin(void_race_ids)].copy()

        after_rows = len(raw_all)
        after_races = raw_all["race_id"].nunique(dropna=True)

        pd.DataFrame([{
            "rows_total": int(before_rows),
            "races_total": int(before_races),
            "void_races": int(len(void_race_ids)),
            "rows_dropped_by_void_race": int(before_rows - after_rows),
            "races_dropped_by_void_race": int(before_races - after_races),
            "rows_after_drop": int(after_rows),
            "races_after_drop": int(after_races),
        }]).to_csv(qc_dir / "qc_void_race_summary.csv", index=False, encoding="utf-8-sig")

        pd.DataFrame({"race_id": void_race_ids}).to_csv(qc_dir / "qc_void_races.csv", index=False, encoding="utf-8-sig")

    if "rank_class" in raw_all.columns:
        (
            raw_all["rank_class"]
            .value_counts(dropna=False)
            .rename_axis("rank_class")
            .reset_index(name="count")
            .to_csv(qc_dir / "qc_rank_class_counts.csv", index=False, encoding="utf-8-sig")
        )

    # ---- Load snapshot & join ----
    print("[INFO] loading snapshot ...")
    snap = read_csv_lenient(snapshot_csv, dtype=str)
    for col in ["date", "code", "motor_number"]:
        if col not in snap.columns:
            raise RuntimeError(f"[ERROR] snapshot missing required column: {col}")

    snap["date_dt"] = to_dt_generic(snap["date"])
    snap["code"] = normalize_code_2d(snap["code"])
    snap["motor_number"] = coerce_int_nullable(snap["motor_number"])

    if "snapshot_id" not in snap.columns:
        snap["snapshot_id"] = snap["date_dt"].dt.strftime("%Y%m%d").astype("string") + "_" + snap["code"].astype("string")

    raw_all["snapshot_id"] = raw_all["date_dt"].dt.strftime("%Y%m%d").astype("string") + "_" + raw_all["code"].astype("string")

    join_keys = ["date_dt", "code", "motor_number"]
    snap = snap.drop_duplicates(subset=join_keys, keep="first").copy()

    snap_add_cols = [c for c in snap.columns if c not in join_keys]
    joined = raw_all.merge(snap[join_keys + snap_add_cols], on=join_keys, how="left", suffixes=("", "__snap"))

    # QC: snapshot join missing
    snap_id_col = "snapshot_id__snap" if "snapshot_id__snap" in joined.columns else "snapshot_id"
    miss = joined[snap_id_col].isna()
    pd.DataFrame([{
        "rows": int(len(joined)),
        "snapshot_join_missing_rows": int(miss.sum()),
        "snapshot_join_missing_rate": float(miss.mean()),
    }]).to_csv(qc_dir / "qc_snapshot_join_missing_overall.csv", index=False, encoding="utf-8-sig")

    # ---- Load motor_id_map and assign motor_id ----
    print("[INFO] loading motor_id_map ...")
    m = read_csv_lenient(map_csv, dtype=str)
    for col in ["code", "motor_number", "effective_from", "effective_to", "motor_id"]:
        if col not in m.columns:
            raise RuntimeError(f"[ERROR] motor_id_map missing '{col}' column")

    m["code"] = normalize_code_2d(m["code"])
    m["motor_number"] = coerce_int_nullable(m["motor_number"])
    m["effective_from_dt"] = to_dt_generic(m["effective_from"])
    m["effective_to_dt"] = to_dt_generic(m["effective_to"])
    m["motor_id"] = m["motor_id"].astype("string")

    # effective_to が欠損なら未来∞として扱う（現役区間対策）
    # ※ 今回の主因は code だが、ここは運用上の安全装置として入れる
    m.loc[m["effective_from_dt"].isna(), "effective_from_dt"] = pd.Timestamp("1900-01-01")
    m.loc[m["effective_to_dt"].isna(), "effective_to_dt"] = pd.Timestamp("2100-12-31")

    joined["motor_id"] = assign_motor_id_by_effective_ranges(
        df=joined,
        map_df=m,
        date_col="date_dt",
        code_col="code",
        motor_number_col="motor_number",
    )

    # QC: motor_id missing
    mid_miss = joined["motor_id"].isna()
    pd.DataFrame([{
        "rows": int(len(joined)),
        "motor_id_missing_rows": int(mid_miss.sum()),
        "motor_id_missing_rate": float(mid_miss.mean()),
    }]).to_csv(qc_dir / "qc_motor_id_missing_overall.csv", index=False, encoding="utf-8-sig")

    # by_code
    tmp = joined[["code", "motor_id"]].copy()
    by_code = (
        tmp.groupby(["code"], dropna=False)
        .apply(lambda g: pd.Series({
            "missing": int(g["motor_id"].isna().sum()),
            "rows": int(len(g)),
        }))
        .reset_index()
    )
    by_code["missing_rate"] = by_code["missing"] / by_code["rows"]
    by_code.to_csv(qc_dir / "qc_motor_id_missing_by_code.csv", index=False, encoding="utf-8-sig")

    # ---- Final dtype hygiene (Parquet安定化) ----
    str_cols = [
        "__source_file", "race_id", "date", "code", "section_id",
        "player_id", "player",
        "rank__raw", "rank_code", "rank_class",
        "ST__raw", "ST_tenji__raw",
        "winning_trick", "weather", "wind_direction", "parts_exchange",
        "snapshot_id", "motor_id",
    ]
    for c in str_cols:
        if c in joined.columns:
            joined[c] = joined[c].astype("string")

    joined["date"] = joined["date_dt"].dt.strftime("%Y%m%d").astype("string")
    joined["date_iso"] = joined["date_dt"].dt.strftime("%Y-%m-%d").astype("string")

    # sample csv
    if args.sample_n and int(args.sample_n) > 0:
        n = min(int(args.sample_n), len(joined))
        joined.sample(n=n, random_state=42).to_csv(qc_dir / f"joined_sample_{n}.csv", index=False, encoding="utf-8-sig")

    # write outputs
    out_csv = out_dir / "raw_with_motor__all.csv"
    out_parquet = out_dir / "raw_with_motor__all.parquet"

    if int(args.write_full_csv) == 1:
        print("[WARN] writing FULL joined CSV (can be huge) ...")
        joined.to_csv(out_csv, index=False, encoding="utf-8-sig")
        print(f"[INFO] wrote FULL joined CSV: {out_csv}")
    else:
        print("[INFO] write_full_csv=0 -> skip FULL CSV")

    print("[INFO] writing parquet ...")
    try:
        joined.to_parquet(out_parquet, index=False)
        print(f"[INFO] wrote parquet: {out_parquet}")
    except Exception as e:
        msg = (
            f"[ERROR] Parquet 出力に失敗しました: {e}\n"
            "原因の典型: pyarrow 未導入、または列型の混在。\n"
            "対処例:\n"
            "  conda install -c conda-forge pyarrow\n"
        )
        raise RuntimeError(msg)

    print(f"[DONE] outputs written under: {out_dir}")


if __name__ == "__main__":
    main()
