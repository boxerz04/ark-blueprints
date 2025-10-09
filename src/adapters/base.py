# -*- coding: utf-8 -*-
"""
Adapter: base (production)

ライブCSVを学習時と同じ前処理に揃える。
- priors の結合（tenji / season_course / winning_trick）
- tenji 残差・Z・レース内ランク・Top1/Top2 を生成
- place/entry/wakuban/season_q のキー型を揃える
- ライブでは entry が未確定のため、「枠なり前提」で entry を wakuban でフォールバック
"""

from __future__ import annotations
from pathlib import Path
from typing import List
import os
import numpy as np
import pandas as pd

# 設定（学習時と合わせる）
TENJI_SD_FLOOR = 0.02


# ========= 共通ユーティリティ =========
def resolve_priors_root(project_root: Path) -> Path:
    """PRIORS_ROOT 環境変数があれば優先。無ければ <project>/data/priors"""
    env = os.environ.get("PRIORS_ROOT", "").strip()
    return Path(env) if env else (project_root / "data" / "priors").resolve()

def to_int_safe(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def to_float_safe(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def season_quarter_from_date(s: pd.Series) -> pd.Series:
    """
    四季（公式規則）
    spring: 03/01–05/31
    summer: 06/01–08/31
    autumn: 09/01–11/30
    winter: 12/01–02/末
    """
    d = pd.to_datetime(s, errors="coerce")
    m = d.dt.month
    out = pd.Series(index=d.index, dtype="object")
    out[(m >= 3) & (m <= 5)] = "spring"
    out[(m >= 6) & (m <= 8)] = "summer"
    out[(m >= 9) & (m <= 11)] = "autumn"
    out[(m == 12) | (m <= 2)] = "winter"
    return out.fillna("autumn").astype(str)

def _dump_csv(df: pd.DataFrame, tag: str):
    """
    環境変数 ADAPTER_DUMP_CSV にファイルパスが設定されていればCSV出力。
    ADAPTER_DUMP_STEPS=1 のときは tag をファイル名に差し込んで段階別に保存。
    例:
      ADAPTER_DUMP_CSV = data\\live\\_debug_merged.csv
      ADAPTER_DUMP_STEPS = 1  -> data\\live\\_debug_merged__post_sc.csv などを吐き分け
    """
    dump_path = os.environ.get("ADAPTER_DUMP_CSV", "").strip()
    if not dump_path:
        return
    base = Path(dump_path)
    if os.environ.get("ADAPTER_DUMP_STEPS", "") == "1":
        out = base.with_name(f"{base.stem}__{tag}{base.suffix}")
    else:
        out = base
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[DBG] dumped adapter df -> {out}")

# ========= priors ロード =========
def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"[adapter] prior not found: {path}")
    return pd.read_csv(path, encoding="utf-8-sig")

def _select_prior_columns(df: pd.DataFrame, key_cols: List[str]) -> pd.DataFrame:
    """
    キー列＋学習で使う数値列のみ残す（メタ列は除外）。
    ※ n_tenji は利用するため除外しない。
    """
    meta_like = {"built_from","built_to","keys","version","sd_floor","m_strength","season_cold_ratio"}
    keep = key_cols.copy()
    for c in df.columns:
        if c in key_cols or c in meta_like:
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            keep.append(c)
    return df[keep].copy()

def load_tenji_prior(priors_root: Path) -> pd.DataFrame:
    p = priors_root / "tenji" / "latest.csv"
    df = _read_csv(p)
    df = _select_prior_columns(df, ["place","wakuban","season_q"])
    # 型寄せ
    df["place"] = df["place"].astype(str)
    df["wakuban"] = to_int_safe(df["wakuban"])
    df["season_q"] = df["season_q"].astype(str)
    # 必須列チェック
    for c in ["tenji_mu","tenji_sd"]:
        if c not in df.columns:
            raise ValueError(f"[adapter] tenji prior lacks column: {c}")
    return df

def load_season_course_prior(priors_root: Path) -> pd.DataFrame:
    p = priors_root / "season_course" / "latest.csv"
    df = _read_csv(p)
    df = _select_prior_columns(df, ["place","entry","season_q"])
    df["place"] = df["place"].astype(str)
    df["entry"] = to_int_safe(df["entry"])
    df["season_q"] = df["season_q"].astype(str)
    return df

def load_winning_trick_prior(priors_root: Path) -> pd.DataFrame:
    p = priors_root / "winning_trick" / "latest.csv"
    df = _read_csv(p)
    df = _select_prior_columns(df, ["place","entry","season_q"])
    df["place"] = df["place"].astype(str)
    df["entry"] = to_int_safe(df["entry"])
    df["season_q"] = df["season_q"].astype(str)
    return df


# ========= 安全マージ（右表は一意である前提を検証） =========
def _assert_right_unique(df_right: pd.DataFrame, on: List[str], tag: str):
    if df_right.duplicated(on).any():
        smp = df_right[df_right.duplicated(on, keep=False)][on].head(6)
        raise ValueError(f"[adapter] RIGHT keys not unique ({tag}) on {on}\n{str(smp)}")

def _merge_left(left: pd.DataFrame, right: pd.DataFrame, on: List[str], suffix: str = "") -> pd.DataFrame:
    _assert_right_unique(right, on, tag="prior")
    # キー型合わせ：place/season_q -> str, entry/wakuban -> Int64
    L = left.copy()
    R = right.copy()
    for k in on:
        if k in {"entry","wakuban"}:
            if k in L.columns:
                L[k] = to_int_safe(L[k])
            if k in R.columns:
                R[k] = to_int_safe(R[k])
        else:
            if k in L.columns:
                L[k] = L[k].astype(str)
            if k in R.columns:
                R[k] = R[k].astype(str)
    return L.merge(R, how="left", on=on, suffixes=("", suffix))


# ========= 展示派生 =========
def add_tenji_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["time_tenji"] = to_float_safe(out.get("time_tenji"))
    out["tenji_mu"]   = to_float_safe(out.get("tenji_mu"))
    out["tenji_sd"]   = to_float_safe(out.get("tenji_sd"))

    resid = out["time_tenji"] - out["tenji_mu"]
    sd    = np.maximum(out["tenji_sd"].fillna(TENJI_SD_FLOOR), TENJI_SD_FLOOR)
    z     = resid / sd

    out["tenji_resid"] = resid
    out["tenji_z"]     = z

    # ランク（NaN は +inf で最下位）
    resid_rank_base = out["tenji_resid"].fillna(np.inf)
    z_rank_base     = out["tenji_z"].fillna(np.inf)

    if "race_id" in out.columns:
        out["tenji_resid_rank"] = resid_rank_base.groupby(out["race_id"]).rank(method="min", ascending=True).astype("Int64")
        out["tenji_z_rank"]     = z_rank_base.groupby(out["race_id"]).rank(method="min", ascending=True).astype("Int64")
        out["tenji_resid_top1"] = (out["tenji_resid_rank"] == 1).astype(int)
        out["tenji_resid_top2"] = (out["tenji_resid_rank"] <= 2).astype(int)
    else:
        out["tenji_resid_rank"] = resid_rank_base.rank(method="min", ascending=True).astype("Int64")
        out["tenji_z_rank"]     = z_rank_base.rank(method="min", ascending=True).astype("Int64")
        out["tenji_resid_top1"] = (out["tenji_resid_rank"] == 1).astype(int)
        out["tenji_resid_top2"] = (out["tenji_resid_rank"] <= 2).astype(int)

    return out


# ========= Public API =========
def prepare_live_input(df_live_raw: pd.DataFrame, project_root: Path) -> pd.DataFrame:
    """
    scripts/predict_one_race.py から呼ばれる入口。
    df_live_raw: 1Rの6行DataFrame（スクレイプ/直前CSV由来）
    ライブでは entry が未確定なので、枠なり前提で entry を wakuban でフォールバックする。
    """
    if df_live_raw is None or len(df_live_raw) == 0:
        return df_live_raw

    df = df_live_raw.copy()

    # 基本型：placeは文字列（キー用途のみ）、wakuban/entry は Int
    if "place" in df.columns:
        df["place"] = df["place"].astype(str)

    if "wakuban" in df.columns:
        df["wakuban"] = to_int_safe(df["wakuban"])

    # entry: ライブでは未確定なので entry_tenji で埋める（既にあれば尊重）
    if "entry" not in df.columns:
        df["entry"] = pd.NA
    df["entry"] = to_int_safe(df["entry"])
    
    # entry_tenji を候補に
    et = to_int_safe(df["entry_tenji"]) if "entry_tenji" in df.columns else pd.Series(pd.NA, index=df.index, dtype="Int64")
    df["entry"] = df["entry"].fillna(et)

    # season_q（date から四季）
    if "date" in df.columns:
        if not np.issubdtype(df["date"].dtype, np.datetime64):
            try:
                df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
            except Exception:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["season_q"] = season_quarter_from_date(df["date"])
    else:
        df["season_q"] = "autumn"

    # priors ロード
    priors_root = resolve_priors_root(project_root)
    tenji_prior = load_tenji_prior(priors_root)
    sc_prior    = load_season_course_prior(priors_root)
    wt_prior    = load_winning_trick_prior(priors_root)

    # 結合（右表は一意、左は 6行想定）
    df = _merge_left(df, tenji_prior, on=["place","wakuban","season_q"])
    df = _merge_left(df, sc_prior,    on=["place","entry","season_q"])
    df = _merge_left(df, wt_prior,    on=["place","entry","season_q"])

    # 付随の型整備（n_tenji は使う）
    if "n_tenji" in df.columns:
        df["n_tenji"] = to_int_safe(df["n_tenji"]).fillna(0).astype(int)

    # 展示派生
    df = add_tenji_features(df)
    _dump_csv(df, "final")

    # 数値列の最終整形
    for c in df.select_dtypes(include=[np.number]).columns.tolist():
        df[c] = to_float_safe(df[c])

    # place/season_q はキー用途の補助列（残しても ColumnTransformer の remainder="drop" が無視）
    return df
