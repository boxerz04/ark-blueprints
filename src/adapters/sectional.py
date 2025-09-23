# src/adapters/sectional.py
# ------------------------------------------------------------
# Sectional モデル用 Adapter（本番推論：ライブHTML直読み＋型統一＋デバッグ出力）
# ------------------------------------------------------------

from __future__ import annotations

from pathlib import Path
import glob
import pandas as pd

from src.raceinfo_features import (
    process_racelist_content,
    calculate_raceinfo_points,
    ranking_point_map,
    condition_point_map,
)

SECTIONAL_10 = [
    "ST_mean_current",
    "ST_rank_current",
    "ST_previous_time",
    "score",
    "score_rate",
    "ranking_point_sum",
    "ranking_point_rate",
    "condition_point_sum",
    "condition_point_rate",
    "race_ct_current",
]
DERIVED_2 = ["ST_previous_time_num", "race_ct_clip6"]
REQUIRED_NUMERIC = SECTIONAL_10 + DERIVED_2


def _find_live_racelist(live_html_dir: Path, race_id: str) -> Path | None:
    """live/html 配下から該当 race_id の 'racelist' を探す（命名・階層の揺れを許容）"""
    rid = str(race_id)
    candidates = []

    patterns = [
        str(live_html_dir / "racelist" / f"racelist{rid}.bin"),         # あなたの環境の実ファイル名
        str(live_html_dir / f"*{rid}*racelist*.bin"),
        str(live_html_dir / f"*racelist*{rid}*.bin"),
        str(live_html_dir / "**" / f"*{rid}*racelist*"),
    ]
    for pat in patterns:
        hits = sorted(glob.glob(pat, recursive=True))
        if hits:
            candidates.extend(hits)

    if not candidates:
        any_racelist = [p for p in (live_html_dir.rglob("*")) if p.is_file() and "racelist" in p.name.lower()]
        if any_racelist:
            candidates = sorted(any_racelist, key=lambda x: x.stat().st_mtime, reverse=True)

    if not candidates:
        return None

    candidates = sorted({Path(p) for p in candidates}, key=lambda x: x.stat().st_mtime, reverse=True)
    return candidates[0]


def _ensure_numeric_neutral(df: pd.DataFrame) -> pd.DataFrame:
    """SECTIONAL 12列を float に統一し、NaN/pd.NA は 0.0 埋め（sklearn対策）"""
    NEUTRAL = 0.0
    for col in REQUIRED_NUMERIC:
        if col not in df.columns:
            df[col] = NEUTRAL
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(NEUTRAL)
    return df


def _add_derived_columns(df: pd.DataFrame) -> None:
    """学習時の派生2列を必ず用意する（in-place）"""
    if "ST_previous_time_num" not in df.columns:
        if "ST_previous_time" in df.columns:
            tail2 = df["ST_previous_time"].fillna("").astype(str).str[-2:]
            df["ST_previous_time_num"] = pd.to_numeric("0." + tail2, errors="coerce")
        else:
            df["ST_previous_time_num"] = pd.NA

    if "race_ct_clip6" not in df.columns:
        if "race_ct_current" in df.columns:
            df["race_ct_clip6"] = pd.to_numeric(df["race_ct_current"], errors="coerce").clip(upper=6)
        else:
            df["race_ct_clip6"] = pd.NA


def prepare_live_input(df_live: pd.DataFrame, project_root: Path) -> pd.DataFrame:
    """
    live(6行) に SECTIONAL の 10列 + 派生2列を付与し、
    最終的に数値統一して返す。
    """
    df = df_live.copy()

    # 必須キー
    need = {"race_id", "player_id"}
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise SystemExit(f"[ERROR] live CSV に必須列が不足しています: {miss}")

    # ★型統一：学習時は player_id, race_id が文字列なので、推論も揃える
    df["player_id"] = df["player_id"].astype(str)
    df["race_id"] = df["race_id"].astype(str)

    race_id = df["race_id"].iloc[0]

    # 1) live racelist を探す
    live_html_dir = project_root / "data" / "live" / "html"
    racelist_path = _find_live_racelist(live_html_dir, race_id)

    if racelist_path is not None and racelist_path.exists():
        try:
            with open(racelist_path, "rb") as f:
                content = f.read()

            # 2) HTML(bytes) → 今節情報 DataFrame
            raceinfo = process_racelist_content(content)  # player_id 等を含む6行
            # 型を学習仕様に合わせる
            if "player_id" in raceinfo.columns:
                raceinfo["player_id"] = raceinfo["player_id"].astype(str)
            raceinfo["race_id"] = str(race_id)

            # 3) score / point 系の列付与 + race_id 付与済み
            raceinfo = calculate_raceinfo_points(
                raceinfo,
                ranking_map=ranking_point_map,
                condition_map=condition_point_map,
                race_id=race_id,
            )

            # 4) merge 前に dtype を念のため表示（デバッグ）
            # print("[DEBUG] dtypes live:\n", df[["player_id","race_id"]].dtypes)
            # print("[DEBUG] dtypes info:\n", raceinfo[["player_id","race_id"]].dtypes)

            use_cols = ["player_id", "race_id"] + [c for c in SECTIONAL_10 if c in raceinfo.columns]
            joined = df.merge(
                raceinfo[use_cols],
                on=["player_id", "race_id"],
                how="left",
                validate="one_to_one",
            )

            # 5) 派生2列を付与
            _add_derived_columns(joined)

            # 6) デバッグ出力（JOIN後の全列を確認）
            debug_out = project_root / "data" / "live" / "debug_sectional_join.csv"
            joined.to_csv(debug_out, index=False, encoding="utf-8-sig")
            print(f"[DEBUG] sectional join result saved: {debug_out}")

            # 7) 必要12列を数値統一
            return _ensure_numeric_neutral(joined)

        except Exception as e:
            # 失敗したときに左右のデバッグCSVを吐く
            print(f"[WARN] live racelist 解析/結合に失敗しました（{racelist_path.name}）: {e} → 中立フォールバック")
            left_dbg = project_root / "data" / "live" / "debug_sectional_left_live.csv"
            right_dbg = project_root / "data" / "live" / "debug_sectional_right_info.csv"
            try:
                df.to_csv(left_dbg, index=False, encoding="utf-8-sig")
                # 右は必要列だけでOK（存在すれば）
                tmp = raceinfo if 'raceinfo' in locals() else pd.DataFrame()
                tmp.to_csv(right_dbg, index=False, encoding="utf-8-sig")
                print(f"[DEBUG] saved debug left/right: {left_dbg} , {right_dbg}")
            except Exception:
                pass

    else:
        print(f"[WARN] live racelist not found for race_id={race_id}  → 中立フォールバック")

    # ---- フォールバック：列だけ作って数値化 ----
    for c in SECTIONAL_10:
        if c not in df.columns:
            df[c] = pd.NA
    _add_derived_columns(df)
    return _ensure_numeric_neutral(df)
