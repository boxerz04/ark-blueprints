# -*- coding: utf-8 -*-
"""
src/rank.py

raceresult の rank（着順）を、モーター節サマリ集計向けに
厳密・再現可能なルールでパースする。

目的:
- 数字以外の値を「欠（DNS）」と「走ったが失格等（DNF/DSQ/FS/LS）」に分離する。
- 追加: '失'（失格）と '＿'（レース不成立を示すプレースホルダ）を分類して unknown を消す。

重要な運用ルール（下流で実施すること）:
- rank_class == "void"（＝ '＿'）が 1 行でも含まれる race_id は「レース不成立」とみなし、
  その race_id の全行をレース単位で除外する（行単位削除はしない）。

rawに出現しうる非数値トークン（単独）:
欠, 妨, エ, 転, 落, 沈, 不, Ｆ, Ｌ, 失, ＿

方針:
- 推測（部分一致・正規表現での拾い上げ）はしない
- ただし表記ゆれ（空白/全角数字/全角F/L/全角スペース）は正規化する
- 正規化後トークンに対して完全一致で分類する
"""

from __future__ import annotations

from typing import Any, Dict, Optional
import numpy as np


# 全角→半角（必要最小限）
_TRANSLATE = str.maketrans(
    {
        "０": "0",
        "１": "1",
        "２": "2",
        "３": "3",
        "４": "4",
        "５": "5",
        "６": "6",
        "Ｆ": "F",
        "Ｌ": "L",
        "　": " ",  # 全角スペース→半角スペース
    }
)

_FINISH_SET = {"1", "2", "3", "4", "5", "6"}

# rank_code -> rank_class
_EVENT_CLASS = {
    # DNS（欠場）
    "欠": "dns",
    # DSQ（走ったが失格）
    "妨": "dsq",
    "エ": "dsq",
    "転": "dsq",
    "落": "dsq",
    "沈": "dsq",
    "失": "dsq",  # 追加: 失格
    # DNF（不完走）
    "不": "dnf",
    # スタート事故
    "F": "fs",
    "L": "ls",
    # レース不成立フラグ（下流で race_id 丸ごと除外に使う）
    "＿": "void",
}


def normalize_rank_token(val: Any) -> str:
    """
    rank_raw を正規化して 1 トークンにする。

    - None/NaN/空文字は ""
    - 全角数字/Ｆ/Ｌ/全角スペースを正規化
    - 前後空白除去
    - 内部の空白は全削除（単独トークン前提の表記ゆれのみ吸収）
      例: " Ｆ " -> "F"
    """
    if val is None:
        return ""

    try:
        if isinstance(val, float) and np.isnan(val):
            return ""
    except Exception:
        pass

    s = str(val)
    s = s.translate(_TRANSLATE).strip()
    if s == "":
        return ""

    # 単独トークンの表記ゆれとしての空白は除去
    s = s.replace(" ", "")
    return s


def parse_rank(val: Any) -> Dict[str, Any]:
    """
    rank を解析し、節サマリ集計に必要な情報を返す。

    Returns:
      {
        rank_raw:   元の文字列
        rank_code:  正規化後トークン
        rank_num:   1..6 のみ float、それ以外 NaN
        rank_class: finish / dns / dsq / dnf / fs / ls / void / unknown
        is_start:   出走扱いか（finish/dsq/dnf/fs/ls は True）
                   dns/void/unknown は False
        is_finish:  完走（着順あり）か（finishのみ True）
      }
    """
    raw = "" if val is None else str(val)
    code = normalize_rank_token(val)

    # 完走（1..6）
    if code in _FINISH_SET:
        return {
            "rank_raw": raw,
            "rank_code": code,
            "rank_num": float(code),
            "rank_class": "finish",
            "is_start": True,
            "is_finish": True,
        }

    cls = _EVENT_CLASS.get(code)
    if cls is not None:
        # 出走扱い（分母に入れる）: dsq/dnf/fs/ls
        # DNS（欠）と void（＿）は分母に入れない
        is_start = cls in {"dsq", "dnf", "fs", "ls"}
        return {
            "rank_raw": raw,
            "rank_code": code,
            "rank_num": np.nan,
            "rank_class": cls,
            "is_start": bool(is_start),
            "is_finish": False,
        }

    # 想定外
    return {
        "rank_raw": raw,
        "rank_code": code,
        "rank_num": np.nan,
        "rank_class": "unknown",
        "is_start": False,
        "is_finish": False,
    }


def rank_num_or_nan(val: Any) -> float:
    """
    rank が 1..6 のときのみ数値を返す。その他は NaN。
    """
    out = parse_rank(val)
    return out["rank_num"] if out["rank_class"] == "finish" else np.nan


def rank_class_to_counts_key(rank_class: str) -> Optional[str]:
    """
    節サマリ集計用のカウントキーへ変換する。
    """
    return {
        "finish": "finish_count",
        "dns": "dns_count",
        "dsq": "dsq_count",
        "dnf": "dnf_count",
        "fs": "false_start_count",
        "ls": "late_start_count",
        "void": "void_race_flag_count",  # 参考: レース単位除外で使うなら集計は任意
    }.get(rank_class)
