# scripts/preprocess_sectional.py
# -----------------------------------------------------------------------------
# master（または live）CSV に「節間（今節スナップショット）」列を “上書き付与” するスクリプト
#
# 方針:
#  - 推論 (--date): racelist HTML(.bin/.html) を raceinfo_features で解析し、
#                   SECTIONAL の 10列を ["player_id","race_id"] で LEFT JOIN
#                   （アダプタ src/adapters/sectional.py と同等の振る舞い）
#  - 学習 (--start/--end): 既存の raceinfo CSV 群を (race_id, player_id) で直接 JOIN（従来どおり）
#  - 列名変換/別名対応はしない（raceinfo_features の出力そのまま）
#  - 余計な派生列は作らない（ST_previous_time_num, race_ct_clip6 などは出力しない）
# -----------------------------------------------------------------------------

import argparse
from pathlib import Path
from typing import List, Optional, Tuple
import pandas as pd
import numpy as np
import glob
import sys

# repo ルートを import パスへ（ファイル直実行でも src が import 可能に）
_PROJECT_ROOT = Path(__file__).resolve().parents[1]  # ark-blueprints/
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# 必須10列（学習・推論でスキーマを揃える）
SECTIONAL_10 = [
    "ST_mean_current","ST_rank_current","ST_previous_time",
    "score","score_rate",
    "ranking_point_sum","ranking_point_rate",
    "condition_point_sum","condition_point_rate",
    "race_ct_current",
]
# 数値化・欠損埋めの対象はこの10列のみ
REQUIRED_NUMERIC = SECTIONAL_10

# 学習JOIN前に落とすリーク列
LEAK_DROP = ["finish1_flag_cur","finish2_flag_cur","finish3_flag_cur"]

# raceinfo_features を import（src/配下 or 直下どちらでも可）
_rif = None
try:
    from src import raceinfo_features as _rif  # type: ignore
except Exception:
    try:
        import raceinfo_features as _rif  # type: ignore
    except Exception:
        _rif = None


# ===== CLI ====================================================================
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="data/processed/master.csv")
    ap.add_argument("--raceinfo-dir", default="data/processed/raceinfo")  # 学習用
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="YYYY-MM-DD")           # 推論（単日）
    g.add_argument("--start-date", help="YYYY-MM-DD")     # 学習（期間）
    ap.add_argument("--end-date", help="YYYY-MM-DD")
    ap.add_argument("--out", default="data/processed/master.csv")
    # 推論時の racelist 探索ルート（既定: data/live/html）
    ap.add_argument("--live-html-root", default="data/live/html")
    return ap.parse_args()


# ===== Helpers ================================================================
def _to_dt(s): return pd.to_datetime(s, errors="coerce")

def _date_range(date: Optional[str], start: Optional[str], end: Optional[str]) -> Tuple[pd.Timestamp, pd.Timestamp]:
    if date:
        d = pd.to_datetime(date); return d.normalize(), d.normalize()
    if start is None or end is None:
        raise ValueError("When --date is not given, both --start-date and --end-date are required.")
    return pd.to_datetime(start).normalize(), pd.to_datetime(end).normalize()

def _list_csvs(root: Path) -> List[Path]:
    return sorted([p for p in root.rglob("*.csv") if p.is_file()])

def _safe_read_csv(path: Path, key_dtypes=True) -> Optional[pd.DataFrame]:
    try:
        if key_dtypes:
            return pd.read_csv(path, low_memory=False, dtype={"race_id":"string","player_id":"string"})
        return pd.read_csv(path, low_memory=False)
    except Exception as e:
        print(f"[WARN] skip read: {path} err={e}")
        return None

def _ensure_sectional_presence(df: pd.DataFrame) -> None:
    for c in SECTIONAL_10:
        if c not in df.columns:
            df[c] = pd.NA

def _ensure_numeric_neutral(df: pd.DataFrame) -> None:
    """必須10列のみ float 化し、欠損は 0.0 で埋める（sklearn/推論安定用）"""
    for col in REQUIRED_NUMERIC:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

def _build_race_id_from_row(row: pd.Series) -> Optional[str]:
    try:
        if pd.notna(row.get("race_id", pd.NA)):
            return str(row["race_id"])
        d = _to_dt(row["date"]); code = str(row["code"]).zfill(2); R = str(row["R"]).zfill(2)
        return f"{d.strftime('%Y%m%d')}{code}{R}"
    except Exception:
        return None

def _find_live_racelist(live_html_dir: Path, race_id: str) -> Optional[Path]:
    """アダプタと同じ探索パターン。命名・階層の揺れを許容して最新版を返す。"""
    rid = str(race_id)
    candidates: List[str] = []
    patterns = [
        str(live_html_dir / "racelist" / f"racelist{rid}.bin"),   # 例: data/live/html/racelist/racelist<rid>.bin
        str(live_html_dir / f"*{rid}*racelist*.bin"),
        str(live_html_dir / f"*racelist*{rid}*.bin"),
        str(live_html_dir / "**" / f"*{rid}*racelist*"),
        str(live_html_dir / "racelist" / f"racelist{rid}.html"),
        str(live_html_dir / f"*{rid}*racelist*.html"),
    ]
    for pat in patterns:
        hits = sorted(glob.glob(pat, recursive=True))
        if hits:
            candidates.extend(hits)
    if not candidates:
        any_racelist = [p for p in (live_html_dir.rglob("*")) if p.is_file() and "racelist" in p.name.lower()]
        candidates = [str(p) for p in sorted(any_racelist, key=lambda x: x.stat().st_mtime, reverse=True)]
    if not candidates:
        return None
    uniq = sorted({Path(p) for p in candidates}, key=lambda x: x.stat().st_mtime, reverse=True)
    return uniq[0]


# ===== 学習：CSV 直接 JOIN =====================================================
def load_raceinfo(dir_path: Path, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    paths = _list_csvs(dir_path)
    if not paths:
        print(f"[INFO] no raceinfo csv under: {dir_path}")
        return pd.DataFrame(columns=["race_id","player_id"])
    outs = []
    for p in paths:
        df = _safe_read_csv(p, key_dtypes=True)
        if df is None or df.empty: continue
        date_col = next((c for c in ("date","yyyymmdd","race_date") if c in df.columns), None)
        if date_col is not None:
            dt = _to_dt(df[date_col]); mask = (dt>=start_ts) & (dt<=end_ts)
            df = df.loc[mask].copy()
            if df.empty: continue
        outs.append(df)
    if not outs:
        return pd.DataFrame(columns=["race_id","player_id"])
    ri = pd.concat(outs, ignore_index=True)
    for k in ("race_id","player_id"):
        if k in ri.columns: ri[k] = ri[k].astype("string").str.strip()
    ri = ri.drop(columns=[c for c in LEAK_DROP if c in ri.columns], errors="ignore")
    return ri

def attach_sectional_direct_train(master: pd.DataFrame, ri: pd.DataFrame) -> pd.DataFrame:
    if {"race_id","player_id"}.issubset(ri.columns):
        ri = ri.drop_duplicates(subset=["race_id","player_id"], keep="last")
    add_cols = [c for c in ri.columns if c not in {"race_id","player_id"} and c not in master.columns]
    small = ri[["race_id","player_id"] + add_cols].copy()
    before = set(master.columns)
    merged = master.merge(small, on=["race_id","player_id"], how="left", validate="many_to_one")
    added = sorted(list(set(merged.columns) - before))
    _ensure_sectional_presence(merged)
    print(f"[INFO] attach_direct added_cols={len(added)}")
    return merged


# ===== 推論：HTML から今節スナップショット付与（アダプタ互換） ======================
def attach_sectional_from_html(master: pd.DataFrame, live_html_root: Path) -> pd.DataFrame:
    if _rif is None:
        print("[WARN] raceinfo_features not found. Fallback to neutral (NaN→0.0).")
        out = master.copy(); _ensure_sectional_presence(out); _ensure_numeric_neutral(out); return out

    # race_id は必須
    r = master.iloc[0]
    race_id = _build_race_id_from_row(r)
    if not race_id:
        print("[WARN] cannot build race_id. Fallback neutral.")
        out = master.copy(); _ensure_sectional_presence(out); _ensure_numeric_neutral(out); return out

    # racelist 検出
    live_html_dir = Path(live_html_root)
    racelist_path = _find_live_racelist(live_html_dir, race_id)
    if racelist_path is None or not racelist_path.exists():
        print(f"[WARN] racelist not found for race_id={race_id}. Fallback neutral.")
        out = master.copy(); _ensure_sectional_presence(out); _ensure_numeric_neutral(out); return out

    print(f"[INFO] racelist detected: {racelist_path}")
    print(f"[DBG] master keys sample: race_id={master.iloc[0]['race_id'] if 'race_id' in master.columns else None}, "
          f"player_ids={sorted(master['player_id'].astype(str).unique().tolist())}")

    # HTML→パース→ポイント計算（1回だけ）
    try:
        content = _rif.load_html(str(racelist_path)) if hasattr(_rif, "load_html") else racelist_path.read_bytes()
        if hasattr(_rif, "process_racelist_content"):
            raceinfo = _rif.process_racelist_content(content)
        elif hasattr(_rif, "parse_racelist_html"):
            raceinfo = _rif.parse_racelist_html(content)
        else:
            raise RuntimeError("raceinfo_features has no parser")

        print(f"[DBG] after parse: cols={list(raceinfo.columns)[:20]}, shape={raceinfo.shape}")

        # 位置引数で呼び出し（引数名差異の影響を避ける）
        raceinfo = _rif.calculate_raceinfo_points(
            raceinfo,
            _rif.ranking_point_map,
            _rif.condition_point_map,
            str(race_id),
        )
        print(f"[DBG] after calc_points: has_race_id={'race_id' in raceinfo.columns}, "
              f"cols={list(raceinfo.columns)[:25]}")
    except Exception as e:
        print(f"[WARN] parse/calc failed: {e}. Fallback neutral.")
        out = master.copy(); _ensure_sectional_presence(out); _ensure_numeric_neutral(out); return out

    # 念のため: race_id 列が無ければ注入（JOINキー保証）
    if "race_id" not in raceinfo.columns:
        print(f"[DBG] injecting race_id={race_id} (calculate_raceinfo_points() did not attach it)")
        raceinfo["race_id"] = str(race_id)

    # JOIN前キー確認
    print(f"[DBG] raceinfo key check: player_id={ 'player_id' in raceinfo.columns }, "
          f"race_id={ 'race_id' in raceinfo.columns }")
    print(f"[DBG] raceinfo race_id uniq={sorted(raceinfo['race_id'].astype(str).unique().tolist())[:3] if 'race_id' in raceinfo.columns else None}")
    print(f"[DBG] raceinfo player_ids={sorted(raceinfo['player_id'].astype(str).unique().tolist())}")

    # JOIN
    out = master.copy()
    for k in ("player_id","race_id"):
        out[k] = out[k].astype("string").str.strip()
        if k in raceinfo.columns:
            raceinfo[k] = raceinfo[k].astype("string").str.strip()

    use_cols = ["player_id","race_id"] + [c for c in SECTIONAL_10 if c in raceinfo.columns]
    joined = out.merge(raceinfo[use_cols], on=["player_id","race_id"], how="left", validate="many_to_one")

    # JOIN結果の当たり具合
    hit = joined["ST_mean_current"].notna().sum() if "ST_mean_current" in joined.columns else 0
    print(f"[DBG] join hits (non-null ST_mean_current): {hit}/{len(joined)}")

    # 数値統一（必須10列のみ）
    _ensure_numeric_neutral(joined)

    # デバッグ保存
    try:
        dbg = Path("data") / "live" / "debug_sectional_join.csv"
        dbg.parent.mkdir(parents=True, exist_ok=True)
        joined.to_csv(dbg, index=False, encoding="utf-8-sig")
        print(f"[DEBUG] sectional join result saved: {dbg}")
    except Exception:
        pass

    return joined


# ===== Main ===================================================================
def main():
    args = parse_args()
    master_path    = Path(args.master)
    out_path       = Path(args.out)
    ri_root        = Path(args.raceinfo_dir)
    live_html_root = Path(args.live_html_root)

    start_ts, end_ts = _date_range(args.date, args.start_date, args.end_date)
    print(f"[INFO] master : {master_path}")
    print(f"[INFO] raceinfo: {ri_root}")
    if args.date: print(f"[INFO] mode   : LIVE (from racelist html)  root={live_html_root}")
    print(f"[INFO] period  : {start_ts.date()} .. {end_ts.date()} (inclusive)")

    master = pd.read_csv(master_path, low_memory=False, dtype={"race_id":"string","player_id":"string"})
    print(f"[INFO] master shape: {master.shape}")
    for k in ("race_id","player_id"):
        if k not in master.columns:
            raise RuntimeError(f"missing key column in master: {k}")
        master[k] = master[k].astype("string").str.strip()

    if args.date:
        merged = attach_sectional_from_html(master, live_html_root)
    else:
        ri = load_raceinfo(ri_root, start_ts, end_ts)
        print(f"[INFO] raceinfo shape: {ri.shape}")
        merged = attach_sectional_direct_train(master, ri) if not ri.empty else master.copy()
        _ensure_sectional_presence(merged)  # 必須10列は最低限存在させる
        _ensure_numeric_neutral(merged)     # 数値統一

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[OK] wrote: {out_path} rows={merged.shape[0]} cols={merged.shape[1]}")

if __name__ == "__main__":
    main()
