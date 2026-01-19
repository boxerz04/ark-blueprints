# -*- coding: utf-8 -*-
"""
src/st.py

ST（スタートタイミング）専用パーサ。

ベース仕様（preprocess.py と同一）:
- 'F.01' -> -0.01
- '0.07' -> +0.07
- 'L.03' -> +0.03
- '3  L' / '3F.01' のような混入は 'L' / 'F.01' に正規化
- それ以外は NaN

追加仕様（ドメイン確定）:
- ST（本番）では 'L' は数値を伴わず単独で現れる（出遅れ失格）
  -> F と同様に「数値化しない」= NaN とする
- ST_tenji（展示）では "コース番号 + 空白 + L" が現れる（例: '4  L'）
  -> この形式のみ +0.45（展示での遅れ代理値）とする
"""

from __future__ import annotations

import re
import numpy as np


TENJI_X_L_VALUE = 0.45  # 展示 ST_tenji の 'X  L'


def parse_st(val, *, is_tenji: bool = False) -> float:
    """
    ST文字列を float に正規化する（不可能/失格扱いは NaN）。

    Parameters
    ----------
    val : Any
        生の ST 文字列
    is_tenji : bool
        True の場合は ST_tenji 用の追加仕様（'X  L' -> +0.45）を有効化
    """
    if val is None:
        return np.nan

    t = str(val).strip()
    if t == "" or t in {"-", "—", "ー", "―"}:
        return np.nan

    # 全角→半角（F/L）
    t = t.replace("Ｆ", "F").replace("Ｌ", "L")

    # ---- 追加仕様（展示）: "X  L" -> +0.45 ----
    # 例: "4  L" / "6  L"
    if is_tenji and re.fullmatch(r"\d+\s*L", t, flags=re.I):
        return TENJI_X_L_VALUE

    # "3  L" / "3F.01" などの混入を "L" / "F.01" に寄せる
    m = re.match(r"^\d+\s*([FL](?:\.\d+)?)$", t, flags=re.I)
    if m:
        t = m.group(1)

    # ---- 本番 ST の 'L' 単独（出遅れ失格）: 数値化しない ----
    # ※ 'F' 単独も同様に数値化しない（従来通り NaN に落ちる）
    if (not is_tenji) and t.upper() == "L":
        return np.nan

    sign = 1.0
    if t[:1].lower() == "f":
        sign, t = -1.0, t[1:].strip()
    elif t[:1].lower() == "l":
        sign, t = 1.0, t[1:].strip()

    # "07" のような2桁のみは 0.07 とみなす
    if re.fullmatch(r"\d{2}", t):
        t = "0." + t

    # ".07" -> "0.07"
    if t.startswith("."):
        t = "0" + t

    # 数値以外は NaN
    if t == "" or not re.fullmatch(r"\d+(\.\d+)?", t):
        return np.nan

    try:
        return sign * float(t)
    except ValueError:
        return np.nan
