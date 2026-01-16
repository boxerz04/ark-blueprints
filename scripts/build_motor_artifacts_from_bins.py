#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rankingmotor*.bin（実体はHTML）から
1) motor_section_snapshot.csv（日次スナップショット）
2) motor_id_map.csv（世代辞書）
を一括生成する（スクリプトは1本、成果物は2つ）。

想定ファイル名例:
  rankingmotor2026010707.bin -> date=20260107, code=07

出力:
- motor_section_snapshot.csv
  snapshot_id, date, code, motor_number, motor_rank, time_zenken, motor_2rentai_rate

  IMPORTANT:
  - snapshot_id は「節（開催期間）」の section_id ではありません。
  - これは rankingmotor の「前検日」基準スナップショットから生成する
    (date, code) の日次IDです。
  - raw 側の正規 section_id（schedule開始日ベース）と混同・衝突しないよう、
    本スクリプトでは section_id 列を完全に廃止しました。

- motor_id_map.csv
  code, motor_number, idx_motor, motor_id, effective_from, effective_to
  effective_to は次の effective_from の前日。最後は NaT（以後有効）を意味する。

重要変更:
- 「紛らわしい section_id」を完全削除（snapshot_id のみ）
- 交換候補検出のデフォルトを transition ON に変更
  - デフォルト: use_transition=True（>0 -> 0 の変化点のみを候補にする）
  - 無効化したい場合: --no_use_transition を指定する

オプション:
- --start_date YYYYMMDD（含む）
- --end_date   YYYYMMDD（含む）
- --gap_days   0%候補日採用間隔（デフォルト180日）
"""

import os
import re
import argparse
from io import StringIO
from typing import Optional, List, Tuple, Dict

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm


# ----------------------------
# ファイル名から date/code 抽出
# ----------------------------
def parse_filename_date_code(filename: str) -> Tuple[str, str]:
    digits = "".join(filter(str.isdigit, filename))
    if len(digits) < 10:
        raise ValueError(f"Filename does not contain enough digits for date+code: {filename}")
    date = digits[:8]
    code = digits[8:10]
    return date, code


# ----------------------------
# MultiIndex 列をフラット化（上段_下段）
# ----------------------------
def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for tup in df.columns:
            parts = []
            for x in tup:
                if x is None:
                    continue
                s = str(x).strip()
                if s == "" or s.lower() == "nan":
                    continue
                parts.append(s)
            new_cols.append("_".join(parts) if parts else "")
        df.columns = new_cols
    else:
        df.columns = [str(c).strip() for c in df.columns]
    return df


# ----------------------------
# HTML table 抽出
# ----------------------------
def read_tables_from_bin(filepath: str) -> List[pd.DataFrame]:
    with open(filepath, "r", encoding="utf-8") as f:
        html = f.read()
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")

    dfs: List[pd.DataFrame] = []
    for table in tables:
        df = pd.read_html(StringIO(str(table)))[0]
        df = flatten_columns(df)
        dfs.append(df)
    return dfs


# ----------------------------
# 便利関数：列探索
# ----------------------------
def _find_exact(cols: List[str], exact: str) -> Optional[str]:
    return exact if exact in cols else None


def _find_contains(cols: List[str], must: List[str], must_not: Optional[List[str]] = None) -> Optional[str]:
    must_not = must_not or []
    for c in cols:
        if all(m in c for m in must) and all(n not in c for n in must_not):
            return c
    return None


# ----------------------------
# 列正規化（順位・モーター番号・複率・前検タイム）
# ----------------------------
def normalize_snapshot_table(df: pd.DataFrame, date_yyyymmdd: str, code_2: str) -> Optional[pd.DataFrame]:
    """
    1つの table を、必要列があれば正規化して返す。

    rankingmotor は「前検日」基準のランキングであり、ここで作れるのは日次スナップショット。
    raw 側の正規 section_id（schedule開始日ベース）とは一切関係がないため、
    本スクリプトでは section_id 列を生成しない。

    必要列（最低限）:
      - モーター番号
      - モーター2連対率
      - 前検タイム
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    cols = df.columns.tolist()

    # MultiIndexフラット化後の典型列名を優先
    rank_col = _find_exact(cols, "順位_順位") or _find_exact(cols, "順位")
    motor_no_col = _find_exact(cols, "モーター_番号") or _find_contains(cols, must=["モーター", "番号"], must_not=["ボート"])
    motor_rate_col = _find_exact(cols, "モーター_2連対率") or _find_contains(cols, must=["モーター", "2連対率"], must_not=["ボート"])
    time_col = (
        _find_exact(cols, "前検タイム_前検タイム")
        or _find_exact(cols, "前検タイム")
        or _find_contains(cols, must=["前検", "タイム"])
    )

    if motor_no_col is None or motor_rate_col is None or time_col is None:
        return None

    if rank_col is None:
        df["_rank_missing"] = pd.NA
        rank_col = "_rank_missing"

    out = pd.DataFrame({
        "motor_rank": df[rank_col],
        "motor_number": df[motor_no_col],
        "motor_2rentai_rate": df[motor_rate_col],
        "time_zenken": df[time_col],
    })

    out["date"] = pd.to_datetime(date_yyyymmdd, format="%Y%m%d", errors="coerce")
    out["code"] = int(code_2)

    # 日次スナップショットID（節ではない）
    out["snapshot_id"] = f"{date_yyyymmdd}_{code_2}"

    def to_int(x):
        if pd.isna(x):
            return pd.NA
        m = re.search(r"\d+", str(x))
        return int(m.group()) if m else pd.NA

    def to_float_percent(x):
        if pd.isna(x):
            return pd.NA
        s = str(x).strip().replace("%", "")
        try:
            return float(s)
        except Exception:
            return pd.NA

    def to_float_time(x):
        if pd.isna(x):
            return pd.NA
        m = re.search(r"(\d+(\.\d+)?)", str(x))
        if not m:
            return pd.NA
        try:
            return float(m.group(1))
        except Exception:
            return pd.NA

    out["motor_rank"] = out["motor_rank"].apply(to_int).astype("Int64")
    out["motor_number"] = out["motor_number"].apply(to_int).astype("Int64")
    out["motor_2rentai_rate"] = out["motor_2rentai_rate"].apply(to_float_percent)
    out["time_zenken"] = out["time_zenken"].apply(to_float_time)

    out = out.dropna(subset=["date", "motor_number"])
    out["motor_number"] = out["motor_number"].astype(int)

    # 列順を固定（誤解しにくい順）
    out = out[[
        "snapshot_id",
        "date",
        "code",
        "motor_number",
        "motor_rank",
        "time_zenken",
        "motor_2rentai_rate",
    ]]

    return out


# ----------------------------
# binファイル一覧を日付で絞る
# ----------------------------
def list_bin_files_filtered(
    bins_dir: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[str]:
    files: List[str] = []
    for f in os.listdir(bins_dir):
        if not f.endswith(".bin"):
            continue
        try:
            date, _ = parse_filename_date_code(f)
        except Exception:
            continue

        if start_date is not None and date < start_date:
            continue
        if end_date is not None and date > end_date:
            continue

        files.append(f)

    files.sort()
    return files


def build_motor_section_snapshot_from_bins(
    bins_dir: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    files = list_bin_files_filtered(bins_dir, start_date=start_date, end_date=end_date)
    if limit is not None:
        files = files[:limit]

    snapshots: List[pd.DataFrame] = []
    for fn in tqdm(files, desc="Reading rankingmotor bins"):
        fp = os.path.join(bins_dir, fn)
        date, code = parse_filename_date_code(fn)

        dfs = read_tables_from_bin(fp)

        found_any = False
        for tdf in dfs:
            norm = normalize_snapshot_table(tdf, date_yyyymmdd=date, code_2=code)
            if norm is not None and len(norm) > 0:
                snapshots.append(norm)
                found_any = True

        if not found_any:
            print(f"[WARN] No usable table found in: {fn}")

    if not snapshots:
        raise RuntimeError(
            "No snapshot rows extracted from bin files. "
            "Likely原因: 列名パターン不一致 or HTML構造変更。"
        )

    snap = pd.concat(snapshots, ignore_index=True)

    # 日次スナップショットの粒度（date, code, motor_number）で重複排除
    snap.sort_values(["date", "code", "motor_number"], inplace=True)
    snap = snap.drop_duplicates(subset=["date", "code", "motor_number"], keep="first").reset_index(drop=True)
    return snap


# ----------------------------
# motor_id_map 生成（候補日 + gap_daysクラスタリング）
# ----------------------------
def build_motor_id_map(
    section_snapshot: pd.DataFrame,
    gap_days: int = 180,
    use_transition: bool = True,
) -> pd.DataFrame:
    df = section_snapshot.copy()
    df = df.dropna(subset=["motor_2rentai_rate"])
    df = df.sort_values(["code", "motor_number", "date"])

    rows: List[Dict] = []

    for (code, mnum), gdf in tqdm(df.groupby(["code", "motor_number"]), desc="Building motor_id_map"):
        gdf = gdf.sort_values("date").copy()
        if len(gdf) == 0:
            continue

        if use_transition:
            prev = gdf["motor_2rentai_rate"].shift(1)
            cur = gdf["motor_2rentai_rate"]
            # (>0 -> 0) の変化点のみを候補にする（誤検出抑制）
            cand_dates = gdf.loc[(cur == 0) & (prev.notna()) & (prev > 0), "date"]
        else:
            # 0%の全観測日を候補にする（高感度）
            cand_dates = gdf.loc[gdf["motor_2rentai_rate"] == 0, "date"]

        # numpy.datetime64 が混ざっても安全に動くように統一
        raw = sorted(pd.to_datetime(pd.unique(cand_dates)))

        filtered: List[pd.Timestamp] = []
        for d in raw:
            if not filtered:
                filtered.append(d)
            else:
                diff_days = (d - filtered[-1]) / pd.Timedelta(days=1)
                if diff_days >= gap_days:
                    filtered.append(d)

        first_obs = pd.to_datetime(gdf["date"].min())

        # 区間開始日（世代1の開始は first_obs を必ず含める）
        starts: List[pd.Timestamp] = [first_obs]
        for d in filtered:
            if d != first_obs:
                starts.append(d)

        starts = sorted(set(starts))

        for i, st in enumerate(starts):
            idx_motor = 1 if i == 0 else (i + 1)

            if i < len(starts) - 1:
                next_st = starts[i + 1]
                ed = next_st - pd.Timedelta(days=1)
            else:
                ed = pd.NaT

            motor_id = f"{int(code):02}{int(mnum):02}{int(idx_motor):02}"
            rows.append({
                "code": int(code),
                "motor_number": int(mnum),
                "idx_motor": int(idx_motor),
                "motor_id": motor_id,
                "effective_from": st,
                "effective_to": ed,
            })

    out = pd.DataFrame(rows)
    out.sort_values(["code", "motor_number", "effective_from"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out


# ----------------------------
# main
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bins_dir", required=True, help="Directory containing rankingmotor*.bin")
    ap.add_argument("--out_snapshot_csv", required=True, help="Output motor_section_snapshot CSV")
    ap.add_argument("--out_map_csv", required=True, help="Output motor_id_map CSV")

    ap.add_argument("--gap_days", type=int, default=180, help="Min days between accepted candidate dates")
    # デフォルトを transition ON にする（OFFにしたい場合のみフラグを付ける）
    ap.add_argument("--no_use_transition", action="store_true",
                    help="Disable transition-based candidates; use all 0%% days as candidates")

    ap.add_argument("--start_date", type=str, default=None, help="Start date YYYYMMDD (inclusive)")
    ap.add_argument("--end_date", type=str, default=None, help="End date YYYYMMDD (inclusive)")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of bin files (debug)")
    args = ap.parse_args()

    if args.start_date is not None and not re.fullmatch(r"\d{8}", args.start_date):
        raise ValueError("--start_date must be YYYYMMDD")
    if args.end_date is not None and not re.fullmatch(r"\d{8}", args.end_date):
        raise ValueError("--end_date must be YYYYMMDD")
    if args.start_date is not None and args.end_date is not None and args.start_date > args.end_date:
        raise ValueError("--start_date must be <= --end_date")

    use_transition = not args.no_use_transition

    target_files = list_bin_files_filtered(args.bins_dir, start_date=args.start_date, end_date=args.end_date)
    if args.limit is not None:
        target_files = target_files[:args.limit]
    if len(target_files) == 0:
        raise RuntimeError("No .bin files matched the specified filters.")

    print(f"[INFO] bins_dir: {args.bins_dir}")
    if args.start_date or args.end_date:
        print(f"[INFO] date filter: {args.start_date or 'MIN'} ~ {args.end_date or 'MAX'}")
    if args.limit is not None:
        print(f"[INFO] limit: {args.limit}")
    print(f"[INFO] files to process: {len(target_files)}")

    snapshot = build_motor_section_snapshot_from_bins(
        args.bins_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        limit=args.limit,
    )

    motor_map = build_motor_id_map(snapshot, gap_days=args.gap_days, use_transition=use_transition)

    os.makedirs(os.path.dirname(args.out_snapshot_csv), exist_ok=True)
    os.makedirs(os.path.dirname(args.out_map_csv), exist_ok=True)

    snapshot.to_csv(args.out_snapshot_csv, index=False, encoding="utf_8_sig")
    motor_map.to_csv(args.out_map_csv, index=False, encoding="utf_8_sig")

    print("[DONE] Outputs saved")
    print(f"  snapshot: {args.out_snapshot_csv}")
    print(f"    rows: {len(snapshot)}")
    print(f"    unique snapshot_id: {snapshot['snapshot_id'].nunique()}")
    print(f"    date range: {snapshot['date'].min().date()} ~ {snapshot['date'].max().date()}")
    print(f"  motor_id_map: {args.out_map_csv}")
    print(f"    rows (intervals): {len(motor_map)}")
    if len(motor_map) > 0:
        print(f"    unique (code,motor_number): {motor_map[['code','motor_number']].drop_duplicates().shape[0]}")
    print(f"  gap_days={args.gap_days}, use_transition={use_transition}")


if __name__ == "__main__":
    main()
