# src/ensemble/meta_features.py
from __future__ import annotations
import pandas as pd

# 期待カラム:
# - 必須: race_id, player_id, y, p_base, p_sectional (p_sectionalはNaNあり得る)
# - 任意: stage, race_attribute 他（あれば使う）
# 返り値: (X: pd.DataFrame, used_cols: list[str])
def build_meta_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    out = pd.DataFrame(index=df.index)

    # 1) 直接予測
    out["p_base"] = df["p_base"].astype(float)
    out["p_sectional"] = df["p_sectional"].astype(float).fillna(0.5)  # 欠損は中立

    # 2) 自信度（マージン）
    out["m_base"] = (out["p_base"] - 0.5).abs()
    out["m_sectional"] = (df["p_sectional"].astype(float).fillna(0.5) - 0.5).abs()

    # 3) 欠損フラグ（sectionalが守備範囲外など）
    out["is_sectional_missing"] = df["p_sectional"].isna().astype(int)

    # 4) 文脈（任意があれば one-hot、なければスキップ）
    cat_used = []
    for cat_col in ["stage", "race_attribute"]:
        if cat_col in df.columns:
            dmy = pd.get_dummies(df[cat_col].astype("category"), prefix=cat_col, dummy_na=False)
            out = pd.concat([out, dmy], axis=1)
            cat_used.extend(list(dmy.columns))

    used_cols = ["p_base", "p_sectional", "m_base", "m_sectional", "is_sectional_missing"] + cat_used
    return out[used_cols], used_cols
