# -*- coding: utf-8 -*-
"""
map_f_to_cols.py

LightGBM が出す f0, f142... の “f-index” を、feature_cols_used.json の列順に照合して
「だいたい何の列か」を特定するユーティリティ。

重要
----
- これは “OneHot展開後の詳細名” を完全復元するものではない。
- ただし f0..f(N_base-1) は、preprocess_base_features.py が pipeline に投入した
  “元列順” を再現できれば、元列名に確実に対応づけられる。

前提（あなたの実行ログ準拠）
----------------------------
pipeline: num_zero=..., num=..., cat=...
→ 入力列順は (num_zero -> num -> cat) を採用する。

使い方（例）
------------
C:\\anaconda3\\python.exe scripts\\map_f_to_cols.py ^
  --feature-cols-json models\\finals\\latest\\feature_cols_used.json ^
  --n-final 198 ^
  --show 0,62,117,142
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Dict


def parse_args() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--feature-cols-json", required=True, help="models/<approach>/latest/feature_cols_used.json")
    ap.add_argument("--n-final", type=int, required=True, help="Final feature dimension (e.g., booster.num_feature())")
    ap.add_argument("--show", default="", help="Comma-separated f-index list to print (e.g., 142,62,117)")
    return ap


def load_json(p: Path) -> Dict:
    if not p.exists():
        raise FileNotFoundError(p)
    return json.loads(p.read_text(encoding="utf-8"))


def main() -> None:
    ap = parse_args()
    args = ap.parse_args()

    j = load_json(Path(args.feature_cols_json))

    num_cols = [str(x) for x in j.get("numeric_cols", [])]
    cat_cols = [str(x) for x in j.get("categorical_cols", [])]
    zero_cols = [str(x) for x in j.get("adv_lr_zero_fill_cols", [])]

    # preprocess_base_features.py の設計（ログ）に合わせた順序
    ordered_num = list(dict.fromkeys(zero_cols + [c for c in num_cols if c not in set(zero_cols)]))
    base_names = ordered_num + cat_cols

    n_base = len(base_names)
    n_final = int(args.n_final)

    print(f"[INFO] base cols (num+cat) = {n_base}")
    print(f"[INFO] final dim (model)  = {n_final}")
    if n_final < n_base:
        raise ValueError(f"n_final({n_final}) < n_base({n_base}) : inconsistent")

    # f-index の一部を表示
    idxs: List[int] = []
    if args.show.strip():
        idxs = [int(x.strip()) for x in args.show.split(",") if x.strip()]

    def name_of(i: int) -> str:
        if i < 0 or i >= n_final:
            return "<out_of_range>"
        if i < n_base:
            return base_names[i]
        # 追加次元は “カテゴリ展開由来” であることだけ示す（詳細復元は不可）
        return f"cat::(expanded)_idx={i - n_base}"

    if idxs:
        print("\n=== lookup ===")
        for i in idxs:
            print(f"f{i}: {name_of(i)}")
        return

    # 全対応表（簡易）を出す：f0..f(n_final-1)
    print("\n=== mapping (head 60) ===")
    for i in range(min(60, n_final)):
        print(f"f{i}: {name_of(i)}")

    if n_final > 60:
        print("... (use --show to lookup specific indices)")


if __name__ == "__main__":
    main()
