# scripts/migrations/2026-01-15_fix_section_id_full_raw.py
# ------------------------------------------------------------
# 目的:
#   data/raw の日次 raw（YYYYMMDD_raw.csv、全場分が1ファイル）について、
#   誤った section_id（YYYYMMDD_code の日別塊）を、
#   schedule の開始日ベース（節開始日）に修正する。
#
# section_id（修正版）:
#   section_id = <節開始日YYYYMMDD>_<場コード2桁>
#
# 例:
#   date=20260114, code=1, schedule="1/12-1/17" -> section_id="20260112_01"
#
# フォールバック:
#   schedule が欠損/壊れて開始日が取れない行は、従来通り date を使う:
#   section_id = <date>_<code2桁>
#
# 変更点（Excel文字化け対策）:
#   - 出力CSVを encoding="utf-8-sig"（BOM付きUTF-8）で保存する。
#     ※Excelのダブルクリックでも文字化けしにくくなる。
#
# 特徴:
#   - 全列を保持して出力する（下流の master 生成に使える）
#   - section_id 計算はベクトル化して高速（applyを使わない）
#
# 実行例（Anaconda Prompt）:
#   python scripts/migrations/2026-01-15_fix_section_id_full_raw.py ^
#     --raw-dir data\raw ^
#     --pattern "202601*_raw.csv" ^
#     --out-dir data\raw_fixed_full_utf8sig ^
#     --dry-run
#
#   python scripts/migrations/2026-01-15_fix_section_id_full_raw.py ^
#     --raw-dir data\raw ^
#     --pattern "*_raw.csv" ^
#     --out-dir data\raw_fixed_full_utf8sig
# ------------------------------------------------------------

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
from tqdm import tqdm


@dataclass
class FileStat:
    file: str
    status: str
    rows: int = 0
    changed_rows: int = 0
    fallback_rows: int = 0
    out: str = ""
    reason: str = ""


def compute_section_id_vectorized(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    date/code/schedule から section_id をベクトル化して生成する。

    戻り値:
      - new_section_id: Series[str]
      - used_fallback: schedule開始日が取れず date を使った行（Series[bool]）

    前提:
      - date: "YYYYMMDD" 文字列/数値混在でもOK（文字列化）
      - code: 1〜24 など（文字列化して2桁ゼロ埋め）
      - schedule: "m/d-m/d" or "mm/dd-mm/dd"
    """
    # 文字列化（空白除去）
    date_s = df["date"].astype(str).str.strip()
    code2 = df["code"].astype(str).str.strip().str.zfill(2)
    sched = df["schedule"].astype(str).str.strip()

    # schedule から開始月日を抽出（start_m, start_d）
    # 例: "1/12-1/17" -> start_m=1, start_d=12
    #     "01/12-01/17" -> start_m=01, start_d=12
    m = sched.str.extract(r"^\s*(\d{1,2})/(\d{1,2})\s*-\s*(\d{1,2})/(\d{1,2})\s*$")
    start_m = pd.to_numeric(m[0], errors="coerce")
    start_d = pd.to_numeric(m[1], errors="coerce")

    # date から年・月を取り出す
    year = pd.to_numeric(date_s.str.slice(0, 4), errors="coerce")
    proc_month = pd.to_numeric(date_s.str.slice(4, 6), errors="coerce")

    # schedule開始日が取れない行（フォールバック対象）
    used_fallback = start_m.isna() | start_d.isna() | year.isna()

    # 年跨ぎ補正:
    # 処理日が1月/2月で、schedule開始月が12月なら前年開始とみなす
    year_adj = year.copy()
    mask_year_cross = (~used_fallback) & (proc_month.isin([1, 2])) & (start_m == 12)
    year_adj.loc[mask_year_cross] = year_adj.loc[mask_year_cross] - 1

    # 節開始日 YYYYMMDD を組み立て
    start_mm = start_m.fillna(1).astype(int).astype(str).str.zfill(2)
    start_dd = start_d.fillna(1).astype(int).astype(str).str.zfill(2)
    start_yyyymmdd = year_adj.fillna(0).astype(int).astype(str) + start_mm + start_dd

    # フォールバック: start_yyyymmdd を date に置換
    start_yyyymmdd = start_yyyymmdd.mask(used_fallback, date_s)

    new_section_id = start_yyyymmdd + "_" + code2
    return new_section_id, used_fallback


def load_csv_full(fp: Path) -> pd.DataFrame:
    """
    全列を読み込む（本番用）。
    dtype を固定しすぎると列が多い場合に地雷になりやすいので、必要列だけ後で文字列化する。
    """
    return pd.read_csv(fp, low_memory=False)


def process_one_file(fp: Path, out_dir: Optional[Path], dry_run: bool) -> FileStat:
    try:
        df = load_csv_full(fp)
    except Exception as e:
        return FileStat(file=str(fp), status="error_read", reason=str(e))

    need = {"date", "code", "schedule"}
    missing = need - set(df.columns)
    if missing:
        return FileStat(file=str(fp), status="skip_missing_cols", reason=f"不足列: {sorted(missing)}", rows=len(df))

    new_section_id, used_fallback = compute_section_id_vectorized(df)

    if "section_id" in df.columns:
        old = df["section_id"].astype(str)
        changed_rows = int((old != new_section_id).sum())
    else:
        changed_rows = int(len(df))

    fallback_rows = int(used_fallback.sum())

    df["section_id"] = new_section_id

    if dry_run:
        return FileStat(
            file=str(fp),
            status="dry_run",
            rows=int(len(df)),
            changed_rows=changed_rows,
            fallback_rows=fallback_rows,
        )

    # 出力先
    if out_dir is None:
        out_path = fp
    else:
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / fp.name

    try:
        # Excel 文字化け対策（BOM付きUTF-8）
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
    except Exception as e:
        return FileStat(file=str(fp), status="error_write", reason=str(e), rows=int(len(df)))

    return FileStat(
        file=str(fp),
        status="ok",
        rows=int(len(df)),
        changed_rows=changed_rows,
        fallback_rows=fallback_rows,
        out=str(out_path),
    )


def write_state_json(state_path: Path, args: argparse.Namespace, stats: List[FileStat]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)

    ok = [s for s in stats if s.status in ("ok", "dry_run")]
    skipped = [s for s in stats if s.status.startswith("skip")]
    errors = [s for s in stats if s.status.startswith("error")]

    payload = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "args": {
            "raw_dir": args.raw_dir,
            "pattern": args.pattern,
            "out_dir": args.out_dir,
            "dry_run": args.dry_run,
        },
        "summary": {
            "files_total": len(stats),
            "files_ok_or_dryrun": len(ok),
            "files_skipped": len(skipped),
            "files_errors": len(errors),
            "rows_total_ok_or_dryrun": int(sum(s.rows for s in ok)),
            "changed_rows_total": int(sum(s.changed_rows for s in ok)),
            "fallback_rows_total": int(sum(s.fallback_rows for s in ok)),
        },
        "skipped": [{"file": s.file, "reason": s.reason} for s in skipped],
        "errors": [{"file": s.file, "reason": s.reason} for s in errors],
    }

    with state_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def print_summary(stats: List[FileStat]) -> None:
    ok = [s for s in stats if s.status in ("ok", "dry_run")]
    skipped = [s for s in stats if s.status.startswith("skip")]
    errors = [s for s in stats if s.status.startswith("error")]

    print("")
    print("========== サマリ ==========")
    print(f"[FILES] total={len(stats)} ok_or_dryrun={len(ok)} skipped={len(skipped)} errors={len(errors)}")
    print(f"[ROWS]  total_ok_or_dryrun={sum(s.rows for s in ok)}")
    print(f"[CHANGED_ROWS] total={sum(s.changed_rows for s in ok)}")
    print(f"[FALLBACK_ROWS] total={sum(s.fallback_rows for s in ok)}  （scheduleが使えずdateにフォールバックした行数）")

    if skipped:
        print("")
        print("---------- スキップ（先頭20件） ----------")
        for s in skipped[:20]:
            print(f"- {s.file} | {s.reason}")

    if errors:
        print("")
        print("---------- エラー（先頭20件） ----------")
        for s in errors[:20]:
            print(f"- {s.file} | {s.reason}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="過去raw（全列）に対して section_id を schedule開始日ベースに修正する（全列保持版 / Excel向けBOM付きUTF-8出力）"
    )
    p.add_argument("--raw-dir", required=True, help="raw CSV が置かれているディレクトリ（例: data/raw）")
    p.add_argument("--pattern", default="*_raw.csv", help="対象ファイルのglobパターン（例: '202601*_raw.csv'）")
    p.add_argument("--out-dir", default="", help="出力先ディレクトリ（空なら上書き。最初は別フォルダ推奨）")
    p.add_argument("--dry-run", action="store_true", help="書き込みは行わず、変更件数の集計だけ行う")
    p.add_argument("--state-path", default="", help="実行結果サマリをJSONで保存するパス（任意）")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    raw_dir = Path(args.raw_dir)
    if not raw_dir.exists():
        raise SystemExit(f"raw-dir が存在しません: {raw_dir}")

    files = sorted(raw_dir.glob(args.pattern))
    if not files:
        raise SystemExit(f"対象ファイルが見つかりません: {raw_dir} / {args.pattern}")

    out_dir = Path(args.out_dir) if args.out_dir.strip() else None

    print("========== 実行条件 ==========")
    print(f"[RAW_DIR]   {raw_dir}")
    print(f"[PATTERN]   {args.pattern}")
    print(f"[OUT_DIR]   {out_dir if out_dir else '(上書き)'}")
    print(f"[DRY_RUN]   {args.dry_run}")
    print("")

    stats: List[FileStat] = []
    for fp in tqdm(files, desc="section_id 修正（全列保持版 / utf-8-sig）"):
        st = process_one_file(fp=fp, out_dir=out_dir, dry_run=args.dry_run)
        stats.append(st)

    print_summary(stats)

    if args.state_path.strip():
        state_path = Path(args.state_path)
        write_state_json(state_path, args, stats)
        print("")
        print(f"[STATE_JSON] 書き出しました: {state_path}")

    if args.dry_run:
        print("")
        print("dry-run モードのため、ファイルの書き換えは行っていません。問題なければ --dry-run を外して実行してください。")


if __name__ == "__main__":
    main()
