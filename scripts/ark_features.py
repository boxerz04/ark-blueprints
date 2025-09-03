# -*- coding: utf-8 -*-
"""
scripts/ark_features.py
- 学習/推論共有の前処理ユーティリティ
- ST_tenji: 文字列を数値に正規化
- ST_tenji_rank: 無ければ生成（昇順=速いほど1位、欠損は7）
"""

import re
import numpy as np
import pandas as pd

__all__ = ["parse_st_value", "st_tenji_to_numeric", "ensure_st_features"]

def parse_st_value(x):
    """'F.09'->-0.09, 'L.05'->+0.05, '.07'->0.07, '0.07'->0.07, その他はNaN"""
    if x is None:
        return np.nan
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return np.nan
    s = str(x).strip()
    if s == "" or s in {"-", "—", "–", "NaN", "nan"}:
        return np.nan
    m = re.match(r"^([FL])\.(\d+)$", s, flags=re.IGNORECASE)
    if m:
        sign = -1.0 if m.group(1).upper() == "F" else 1.0
        return sign * float("0." + m.group(2))
    if re.match(r"^\.\d+$", s):
        return float("0" + s)
    try:
        return float(s)
    except Exception:
        return np.nan

def st_tenji_to_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """ST_tenji を数値化（列は増やさない）"""
    out = df.copy()
    if "ST_tenji" in out.columns:
        out["ST_tenji"] = out["ST_tenji"].apply(parse_st_value).astype(float)
    return out

def ensure_st_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    ST_tenji → 数値化し、ST_tenji_rank が無ければ生成して付与。
    学習データにすでに ST_tenji_rank がある場合は上書きしない（そのまま維持）。
    """
    out = st_tenji_to_numeric(df)
    if "ST_tenji_rank" not in out.columns and "ST_tenji" in out.columns:
        st = pd.to_numeric(out["ST_tenji"], errors="coerce")
        rank = st.rank(method="min", ascending=True)  # 速いほど1
        # 学習で Int64 を使っても最終的にスケーラに入るので float でよい
        out["ST_tenji_rank"] = rank.fillna(7).astype(float)
    return out
