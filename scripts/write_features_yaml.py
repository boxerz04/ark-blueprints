# -*- coding: utf-8 -*-
"""
write_features_yaml.py (robust; generate YAML from feature_cols_used.json)

目的
----
preprocess_base_features.py が出力する feature_cols_used.json を「真実の一次情報」として、
学習で採用された feature columns を YAML（ホワイトリスト）として書き出す。

このスクリプトが解決すること
----------------------------
- 手動コピペによる列名ミス（タイプミス / 抜け / 重複 / 順序崩れ）を防ぐ
- “最初の features/finals.yaml を作る” という退屈で危険な作業を自動化する
- 生成後は YAML を手で削る（or コメントアウト）だけで列整理の試行ができる

基本思想
--------
- feature_cols_used.json は「実際に前処理が採用した列の一覧」であり、事実（Truth）である
- features/*.yaml は「今後もこの列を使う」という設計（Design）である
- まず Truth から Design を自動生成し、その後の試行は Design を編集して回す

入力（例）
----------
models/finals/latest/feature_cols_used.json
  - selected_feature_cols: list[str] が必須

出力（例）
----------
features/finals.yaml
  - YAML 形式で columns を出力
  - 既存ファイルがある場合は上書き（--force）または拒否

YAML形式
--------
本プロジェクトで扱いやすいよう、以下の形式で出力する：

columns:
  - colA
  - colB
  - ...

※ 将来、preprocess_base_features.py 側が (A) columns: [...] 形式を読む対応をしている前提。

使い方（概要）
--------------
1) まず YAML なしで preprocess_base_features.py を一度回し、feature_cols_used.json を作る
2) 本スクリプトで YAML を自動生成
3) YAML を編集（削る/コメントアウト）し、以後の学習は YAML 固定で回す
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any, Optional


# =============================================================================
# CLI
# =============================================================================

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Generate features YAML from feature_cols_used.json (Truth -> Design)."
    )
    ap.add_argument(
        "--in-json",
        required=True,
        help="Input feature_cols_used.json (e.g., models/finals/latest/feature_cols_used.json)"
    )
    ap.add_argument(
        "--out-yaml",
        required=True,
        help="Output YAML path (e.g., features/finals.yaml)"
    )
    ap.add_argument(
        "--key",
        default="selected_feature_cols",
        help="JSON key that holds feature columns list (default: selected_feature_cols)"
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Overwrite out-yaml if exists (default: refuse)"
    )
    ap.add_argument(
        "--sort",
        action="store_true",
        help="Sort columns alphabetically (default: keep original order from json)"
    )
    ap.add_argument(
        "--header-comment",
        default="",
        help="Optional comment header to include at top of YAML"
    )
    return ap


# =============================================================================
# Helpers
# =============================================================================

def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"[ERROR] input json not found: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise SystemExit(f"[ERROR] invalid json: {path}\n{e}")


def normalize_cols(cols: List[Any]) -> List[str]:
    """
    - str化
    - 空白除去
    - 空文字除外
    - 重複除外（順序維持）
    """
    out: List[str] = []
    seen = set()
    for x in cols:
        s = str(x).strip()
        if not s:
            continue
        if s in seen:
            continue
        out.append(s)
        seen.add(s)
    return out


def dump_yaml_columns(
    cols: List[str],
    out_path: Path,
    force: bool,
    header_comment: str = ""
) -> None:
    if out_path.exists() and not force:
        raise SystemExit(f"[ERROR] out yaml already exists: {out_path}\n"
                         f"       Use --force to overwrite.")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines: List[str] = []

    # 任意のヘッダーコメント（複数行OK）
    if header_comment:
        for line in header_comment.splitlines():
            line = line.rstrip("\n")
            if not line.startswith("#"):
                line = "# " + line
            lines.append(line)
        lines.append("")  # 空行

    # 自動生成のメタコメント（事故防止）
    lines.append("# AUTO-GENERATED: edit by removing/commenting out columns you don't want.")
    lines.append("# Source of truth: feature_cols_used.json (selected_feature_cols)")
    lines.append("columns:")

    for c in cols:
        # YAML的に危ない文字が含まれる可能性は低いが、念のためクォートするほどでもない。
        # もし ':' や '#' を含む列名が将来出た場合は、ここでクォート処理を追加する。
        lines.append(f"  - {c}")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    args = build_parser().parse_args()

    in_json = Path(args.in_json)
    out_yaml = Path(args.out_yaml)
    key = args.key

    data = load_json(in_json)

    if key not in data:
        # 何が入っているか見えるようにキー一覧を提示する
        keys = list(data.keys())
        raise SystemExit(f"[ERROR] key '{key}' not found in json: {in_json}\n"
                         f"       available keys: {keys}")

    raw_cols = data.get(key)
    if not isinstance(raw_cols, list):
        raise SystemExit(f"[ERROR] json['{key}'] is not a list: {type(raw_cols)}")

    cols = normalize_cols(raw_cols)

    if args.sort:
        cols = sorted(cols)

    if not cols:
        raise SystemExit("[ERROR] no columns found after normalization.")

    # 生成対象が “finals.yaml” などの場合、誤って base.yaml を上書きしないように軽く注意
    # （強制はしないがログで見えるようにする）
    print(f"[INFO] in-json : {in_json}")
    print(f"[INFO] out-yaml: {out_yaml}")
    print(f"[INFO] key     : {key}")
    print(f"[INFO] cols    : {len(cols)} (sorted={bool(args.sort)})")

    dump_yaml_columns(
        cols=cols,
        out_path=out_yaml,
        force=bool(args.force),
        header_comment=args.header_comment or "",
    )

    print(f"[OK] wrote: {out_yaml}")


if __name__ == "__main__":
    main()
