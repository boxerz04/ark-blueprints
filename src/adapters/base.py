# src/adapters/base.py
from pathlib import Path
import pandas as pd

def prepare_live_input(df_live: pd.DataFrame, project_root: Path) -> pd.DataFrame:
    """
    base モデル用：live(6行) をそのまま返すだけ。
    追加の JOIN や派生は不要。
    """
    return df_live
