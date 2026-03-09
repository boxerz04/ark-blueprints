# gui_predict_one_race.py
# ------------------------------------------------------------
# 1レース推論 GUI（approach 切替 / スクレイプ or 既存CSV / CSV自動推定 / 列情報表示オプション）
#
# フロー（従来）:
#   scrape_one_race -> build_live_row -> preprocess_course -> preprocess_sectional -> predict_one_race
#
# ★追加（2026-02-02〜）
# - live CSV に motor 特徴量を付与できるよう、学習と同じ2段をパイプラインへ挿入：
#     1) preprocess_motor_id.py        : date/code/motor_number から motor_id を付与
#     2) preprocess_motor_section.py   : motor_id + section_id で motor_section_features を join（motor_列を追加）
#
# - 堅牢優先（運用サイクル化のための方針）：
#     - motor_id 付与が失敗した場合、motor_section は実行せずスキップして継続
#     - motor_section が失敗した場合もスキップして継続
#     - ただしログに「motor skipped（理由）」を明記する
#
# - 重要：latest（motor特徴量あり）モデルでは、motor_* 列が入力に存在しないと
#         sklearn ColumnTransformer が transform 時に例外を投げて推論できない。
#         したがって motor をスキップする場合でも、motor_* 列“だけは” NaN で作成して入力へ補う。
#         （値は NaN のままでOK。学習側の補完ロジック or LightGBM の欠損扱いで吸収する想定）
#
# - 重要：build_live_row.py が date 列を YYYYMMDD（例: 20260202）の形で出す場合がある。
#         preprocess_motor_id.py は pd.to_datetime() を素直に適用するため、
#         数値の YYYYMMDD が意図通り「2026-02-02」と解釈されず、期間判定が全滅→motor_id 100%欠損になり得る。
#         そのため、motor ステップの直前で date を YYYY-MM-DD へ正規化する “極小ステップ” を追加する。
#         旧モデル（motor無し）でも date は学習特徴量に使われないため、この正規化は安全。
#
# - ログ永続化：
#     data/live/logs/gui_predict_YYYYMMDD_HHMMSS.log に追記保存
# ------------------------------------------------------------

import os
import sys
import re
import json
import queue
import signal
import threading
import subprocess
import locale
from datetime import datetime, timezone, timedelta
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# pandas は “date正規化” と “motor列補完” のために使用
# （GUIの見た目/挙動には影響しない）
import pandas as pd


# ====== 定数 ======
APP_TITLE = "Boatrace 1レース推論 GUI（base/sectional + CSV自動推定 + 列一覧オプション）"
SETTINGS_FILE = os.path.join("data", "config", "settings.json")
LEGACY_SETTINGS_FILE = "settings.json"  # 旧互換

JCD_CHOICES  = [f"{i:02d}" for i in range(1, 25)]
RACE_CHOICES = [f"{i}" for i in range(1, 13)]
APPROACH_CHOICES = ["base", "sectional"]

# motor の live 運用方針
# - map未反映/交換直後などがあり得るため、live は “落とさない” を優先する。
# - preprocess_motor_id.py の max_miss_rate は「%（0〜100）」で評価される実装なので、
#   live では 100（=100%欠損でも許容）に寄せる。
#   ※ “欠損でも許容” の目的は「推論を止めない」こと。
#      latestモデルでは列補完も必要（後述）。
LIVE_MOTOR_ID_MAX_MISS_RATE = 100.0

# motor_section 特徴量（常に最新を想定）
MOTOR_SECTION_FEATURES_CSV = os.path.join("data", "processed", "motor", "motor_section_features_n__all.csv")

# ログ保存先（1実行=1ファイル）
LIVE_LOG_DIR = os.path.join("data", "live", "logs")

SCRIPTS = {
    "scrape_one_race": os.path.join("scripts", "scrape_one_race.py"),
    "build_live_row":  os.path.join("scripts", "build_live_row.py"),
    "predict_one_race": os.path.join("scripts", "predict_one_race.py"),
    "preprocess_course": os.path.join("scripts", "preprocess_course.py"),
    "preprocess_sectional": os.path.join("scripts", "preprocess_sectional.py"),

    # motor
    "preprocess_motor_id": os.path.join("scripts", "preprocess_motor_id.py"),
    "preprocess_motor_section": os.path.join("scripts", "preprocess_motor_section.py"),
}


# ====== ユーティリティ ======
def ensure_parent_dir(path: str):
    parent = Path(path).parent
    if str(parent) not in ("", "."):
        parent.mkdir(parents=True, exist_ok=True)
def load_settings() -> dict:
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    if os.path.exists(LEGACY_SETTINGS_FILE):
        try:
            with open(LEGACY_SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            ensure_parent_dir(SETTINGS_FILE)
            with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return data
        except Exception:
            return {}
    return {}

def save_settings(state: dict):
    try:
        ensure_parent_dir(SETTINGS_FILE)
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def today_jst_yyyymmdd() -> str:
    jst = timezone(timedelta(hours=9))
    return datetime.now(jst).strftime("%Y%m%d")

def valid_yyyymmdd(s: str) -> bool:
    if not s or len(s) != 8 or not s.isdigit():
        return False
    try:
        datetime.strptime(s, "%Y%m%d")
        return True
    except ValueError:
        return False

def now_jst_timestamp() -> str:
    jst = timezone(timedelta(hours=9))
    return datetime.now(jst).strftime("%Y%m%d_%H%M%S")


# ====== 追加：date 正規化（YYYYMMDD → YYYY-MM-DD） ======
def normalize_date_column_inplace(csv_path: str, log_fn=None) -> bool:
    """
    live CSV の date 列を正規化する（極小ステップ）
    - build_live_row.py が date=YYYYMMDD（例: 20260202）を出すケースを吸収する
    - preprocess_motor_id.py は pd.to_datetime() を素直に適用するため、
      数値YYYYMMDDを意図通り解釈できないケースがある。
    - 旧モデルは date を特徴量に使わないため、この正規化は安全。

    挙動：
    - date 列が存在しない → 何もしない（False）
    - date の先頭値が YYYY-MM-DD っぽい → 何もしない（True）
    - date が 8桁数字なら format=%Y%m%d でパースし YYYY-MM-DD に変換
    - 変換に失敗（NaTが発生）した場合も“推論は止めない”方針のため、元の値を残しつつ WARN を出す

    Returns:
        True  : 処理を試みた（変換した/しなかった含む）
        False : date列が無く、何もしなかった
    """
    if not os.path.exists(csv_path):
        if log_fn:
            log_fn(f"[WARN] date normalize skipped: file not found: {csv_path}")
        return False

    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except Exception as e:
        if log_fn:
            log_fn(f"[WARN] date normalize skipped: read failed: {e}")
        return False

    if "date" not in df.columns:
        if log_fn:
            log_fn("[INFO] date normalize: no 'date' column (skipped)")
        return False

    # 先頭値でざっくり判定（全行同形式で出る想定）
    head = df["date"].iloc[0]
    s_head = "" if pd.isna(head) else str(head).strip()

    # 既に YYYY-MM-DD なら何もしない
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s_head):
        if log_fn:
            log_fn(f"[INFO] date normalize: already ISO (sample={s_head})")
        return True

    # 8桁数字（YYYYMMDD）として解釈できるものは変換する
    # ※ float になって "20260202.0" のように見えるケースもあるので .0 を除去
    s = df["date"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    is_yyyymmdd_like = s.str.fullmatch(r"\d{8}", na=False)
    if is_yyyymmdd_like.any():
        # 該当行だけ変換（他はそのまま）
        parsed = pd.to_datetime(s.where(is_yyyymmdd_like, pd.NA), format="%Y%m%d", errors="coerce")
        # 変換できた行のみ YYYY-MM-DD へ
        iso = parsed.dt.strftime("%Y-%m-%d")

        # 失敗行（coerce→NaT）がある場合でも止めない（堅牢優先）
        bad = iso.isna() & is_yyyymmdd_like
        if bad.any() and log_fn:
            n_bad = int(bad.sum())
            log_fn(f"[WARN] date normalize: failed to parse {n_bad} rows as YYYYMMDD (kept original)")

        # 反映（成功した行だけ上書き）
        df.loc[iso.notna(), "date"] = iso[iso.notna()]

        # 上書き保存（utf-8-sig を維持）
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

        if log_fn:
            # 変換後の先頭を再表示
            sample2 = str(df["date"].iloc[0])
            log_fn(f"[INFO] date normalize: converted (sample={s_head} -> {sample2})")
        return True

    # ここまで来たら “8桁数字ではないが ISOでもない” 形式
    # motor_id 付与に影響する可能性はあるが、GUI側で勝手に変形すると危険なのでここでは触らない。
    if log_fn:
        log_fn(f"[WARN] date normalize: unknown format (sample={s_head}) (left as-is)")
    return True


# ====== 追加：motor列の補完（motorスキップ時に列名だけ揃える） ======
def ensure_motor_feature_columns_inplace(csv_path: str, motor_features_csv: str, log_fn=None) -> bool:
    """
    motor step をスキップした場合でも、latest（motorあり）モデルで transform が落ちないように、
    motor_* 列を NaN 列として補完する。

    - motor_section_features_n__all.csv の “特徴量列” を読み取り、
      live CSV に無い列は motor_ 接頭辞付きで追加（値は NaN）
    - 既に存在する列は触らない

    Returns:
        True  : 補完を実施/確認した
        False : motor_features_csv が存在しない等で何もしなかった
    """
    if not os.path.exists(csv_path):
        if log_fn:
            log_fn(f"[WARN] motor col fill skipped: live csv not found: {csv_path}")
        return False
    if not os.path.exists(motor_features_csv):
        if log_fn:
            log_fn(f"[WARN] motor col fill skipped: motor features not found: {motor_features_csv}")
        return False

    try:
        df_live = pd.read_csv(csv_path, low_memory=False)
    except Exception as e:
        if log_fn:
            log_fn(f"[WARN] motor col fill skipped: read live failed: {e}")
        return False

    try:
        # ヘッダだけ読む（高速）
        df_feat0 = pd.read_csv(motor_features_csv, nrows=0)
        feat_cols = list(df_feat0.columns)
    except Exception as e:
        if log_fn:
            log_fn(f"[WARN] motor col fill skipped: read motor header failed: {e}")
        return False

    # motor_section_features 側の “キー・メタ列” は除外して、残りを特徴量扱いにする
    # ※ 実データに合わせて増減しても壊れにくいよう、保守的に除外する。
    meta_like = {
        "code", "motor_number", "idx_motor", "motor_id",
        "section_id", "section_start_dt", "section_end_dt",
        "effective_from", "effective_to",
    }
    raw_feature_cols = [c for c in feat_cols if c not in meta_like]

    # live 側に追加すべき列名（接頭辞 motor_）
    need_cols = [f"motor_{c}" for c in raw_feature_cols]

    missing = [c for c in need_cols if c not in df_live.columns]
    if not missing:
        if log_fn:
            log_fn(f"[INFO] motor col fill: OK (no missing motor_* cols) cols={len(need_cols)}")
        return True

    # 欠けている motor_* 列を NaN で追加
    for c in missing:
        df_live[c] = pd.NA

    # 保存（上書き）
    df_live.to_csv(csv_path, index=False, encoding="utf-8-sig")

    if log_fn:
        log_fn(f"[INFO] motor col fill: added {len(missing)} cols as NaN (total motor feat cols={len(need_cols)})")
    return True


# ====== 実行ランナー ======
class Runner:
    def __init__(self, log_queue: queue.Queue):
        self.log_queue = log_queue
        self.stop_flag = threading.Event()
        self.current_proc = None
        self.log_file_path = None

    def _log(self, text: str):
        """GUIログ（queue）へ送る。必要ならログファイルにも追記する。"""
        self.log_queue.put(text)
        if self.log_file_path:
            try:
                ensure_parent_dir(self.log_file_path)
                with open(self.log_file_path, "a", encoding="utf-8", errors="replace") as f:
                    f.write(text + "\n")
            except Exception:
                pass

    def stop(self):
        self.stop_flag.set()
        try:
            if self.current_proc and self.current_proc.poll() is None:
                if os.name == "nt":
                    self.current_proc.send_signal(signal.CTRL_BREAK_EVENT)
                self.current_proc.terminate()
        except Exception:
            pass

    def _run_and_stream(self, cmd, cwd=None, env=None):
        """サブプロセスを起動し stdout/stderr を逐次GUIへ流す。"""
        if self.stop_flag.is_set():
            return 1
        enc = locale.getpreferredencoding(False)
        self._log(f"\n$ {' '.join(map(str, cmd))}\n")

        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        preexec_fn = None
        if os.name != "nt":
            import os as _os
            preexec_fn = _os.setsid

        self.current_proc = subprocess.Popen(
            cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, universal_newlines=True,
            encoding=enc, errors="replace",
            creationflags=creationflags, preexec_fn=preexec_fn,
            env=env
        )

        for raw in self.current_proc.stdout:
            line = (raw or "").rstrip("\n")
            self._log(line)

        rc = self.current_proc.wait()
        self.current_proc = None
        self._log(f"[exit code] {rc}\n")
        return rc

    def run_pipeline(self,
                     date_yyyymmdd: str, jcd: str, race: str,
                     approach: str, model_dir: str,
                     use_online: bool,
                     use_csv: bool, csv_path: str, csv_autoguess: bool,
                     show_features: bool,
                     repo_root: str,
                     dump_debug: bool = False):

        # この実行のログファイルを確定
        self.log_file_path = os.path.join(LIVE_LOG_DIR, f"gui_predict_{now_jst_timestamp()}.log")
        self._log(f"[INFO] log file: {self.log_file_path}")

        # スクリプト存在チェック（motor追加分含む）
        for k in ("scrape_one_race","build_live_row","predict_one_race",
                  "preprocess_course","preprocess_sectional",
                  "preprocess_motor_id","preprocess_motor_section"):
            if k in SCRIPTS and not os.path.exists(SCRIPTS[k]):
                self._log(f"ERROR: {SCRIPTS[k]} が見つかりません。リポジトリ直下で実行してください。")
                return

        # 1) 入力CSVを決める
        in_csv = None
        if use_csv:
            if csv_autoguess:
                guessed = os.path.join("data","live", f"raw_{date_yyyymmdd}_{jcd}_{race}.csv")
                if os.path.exists(guessed):
                    in_csv = guessed
                    self._log(f"[INFO] CSV自動推定: {guessed}")
                else:
                    self._log(f"[WARN] CSV自動推定: 見つかりません -> {guessed}")
                    if csv_path and os.path.exists(csv_path):
                        in_csv = csv_path
                        self._log(f"[INFO] フォールバック: 明示指定CSVを使用 -> {csv_path}")
                    else:
                        self._log("[ERROR] 入力CSVが確定できません（自動推定失敗かつ明示指定なし/不在）")
                        return
            else:
                if not csv_path or not os.path.exists(csv_path):
                    self._log("[ERROR] 既存CSVを使用にチェックありですが、CSVパスが不正です。")
                    return
                in_csv = csv_path
                self._log(f"[INFO] 明示指定CSVを使用: {csv_path}")
        else:
            # 2) scrape
            rc = self._run_and_stream(
                [sys.executable, SCRIPTS["scrape_one_race"], "--date", date_yyyymmdd, "--jcd", jcd, "--race", race],
                cwd=repo_root
            )
            if rc != 0 or self.stop_flag.is_set():
                return

            # 3) build_live_row
            out_csv = os.path.join("data","live", f"raw_{date_yyyymmdd}_{jcd}_{race}.csv")
            cmd2 = [sys.executable, SCRIPTS["build_live_row"],
                    "--date", date_yyyymmdd, "--jcd", jcd, "--race", race, "--out", out_csv]
            if use_online:
                cmd2.append("--online")
            rc = self._run_and_stream(cmd2, cwd=repo_root)
            if rc != 0 or self.stop_flag.is_set():
                return
            in_csv = out_csv

        if not in_csv:
            self._log("[ERROR] 内部エラー: in_csv が未確定です。")
            return

        # ---------------------------------------------------------------------
        # ★追加：date 正規化（YYYYMMDD → YYYY-MM-DD）
        # - motor_id 付与の前提を満たすため、ここで必ず一度正規化を試みる
        # - 旧モデルでも安全（date を特徴量に使わない想定）
        # ---------------------------------------------------------------------
        normalize_date_column_inplace(in_csv, log_fn=self._log)

        # ---------------------------------------------------------------------
        # ★motor 特徴量（学習と同じ2段を live CSV に上書き付与）
        # - preprocess_motor_id.py      : date/code/motor_number から motor_id を付与
        # - preprocess_motor_section.py : motor_id + section_id で motor_section_features を join（motor_列を追加）
        #
        # 堅牢優先の運用方針：
        # - motor が落ちても推論を止めない
        # - ただし latest（motorあり）モデルは列不足で落ちるため、motor_* 列を NaN で補完する
        # ---------------------------------------------------------------------
        motor_enabled = True
        motor_skip_reason = ""

        # (A) motor_id 付与（liveは落とさない：max_miss_rate=100%）
        cmd_m1 = [
            sys.executable, SCRIPTS["preprocess_motor_id"],
            "--in_csv", in_csv,
            "--out_csv", in_csv,  # 上書き
            "--max_miss_rate", str(LIVE_MOTOR_ID_MAX_MISS_RATE),
        ]
        rc = self._run_and_stream(cmd_m1, cwd=repo_root)
        if rc != 0 or self.stop_flag.is_set():
            motor_enabled = False
            motor_skip_reason = f"preprocess_motor_id failed (exit={rc})"
            if self.stop_flag.is_set():
                return
            self._log(f"[WARN] motor skipped: {motor_skip_reason}")
        else:
            # (B) motor_section join
            if not os.path.exists(MOTOR_SECTION_FEATURES_CSV):
                motor_enabled = False
                motor_skip_reason = f"motor_section_features not found: {MOTOR_SECTION_FEATURES_CSV}"
                self._log(f"[WARN] motor skipped: {motor_skip_reason}")
            else:
                cmd_m2 = [
                    sys.executable, SCRIPTS["preprocess_motor_section"],
                    "--master_csv", in_csv,
                    "--motor_section_csv", MOTOR_SECTION_FEATURES_CSV,
                    "--out_master_csv", in_csv,  # 上書き
                ]
                rc2 = self._run_and_stream(cmd_m2, cwd=repo_root)
                if rc2 != 0 or self.stop_flag.is_set():
                    motor_enabled = False
                    motor_skip_reason = f"preprocess_motor_section failed (exit={rc2})"
                    if self.stop_flag.is_set():
                        return
                    self._log(f"[WARN] motor skipped: {motor_skip_reason}")
                else:
                    self._log("[INFO] motor features: OK (motor_id + motor_section joined)")

        # motor をスキップした場合でも、latest（motorあり）モデルで落ちないよう列だけ補完する
        if not motor_enabled:
            ensure_motor_feature_columns_inplace(
                in_csv,
                motor_features_csv=MOTOR_SECTION_FEATURES_CSV,
                log_fn=self._log
            )

        # ---------------------------------------------------------------------
        # preprocess_course / preprocess_sectional（従来フロー）
        # ---------------------------------------------------------------------
        DEFAULT_WARMUP_DAYS = 180
        DEFAULT_N_LAST = 10

        # YYYYMMDD → YYYY-MM-DD
        y, m, d = date_yyyymmdd[:4], date_yyyymmdd[4:6], date_yyyymmdd[6:]
        start_str = f"{y}-{m}-{d}"
        end_str   = f"{y}-{m}-{d}"

        reports_dir = os.path.join("data", "processed", "course_meta_live")
        ensure_parent_dir(os.path.join(reports_dir, "_dummy.txt"))

        cmd_pc = [
            sys.executable, SCRIPTS["preprocess_course"],
            "--master", in_csv,
            "--raw-dir", os.path.join("data", "raw"),
            "--out", in_csv,
            "--reports-dir", reports_dir,
            "--start-date", start_str,
            "--end-date",   end_str,
            "--warmup-days", str(DEFAULT_WARMUP_DAYS),
            "--n-last",       str(DEFAULT_N_LAST),
        ]
        rc = self._run_and_stream(cmd_pc, cwd=repo_root)
        if rc != 0 or self.stop_flag.is_set():
            return

        cmd_ps = [
            sys.executable, SCRIPTS["preprocess_sectional"],
            "--master", in_csv,
            "--raceinfo-dir", os.path.join("data", "processed", "raceinfo"),
            "--date", date_yyyymmdd,
            "--live-html-root", os.path.join("data","live","html"),
            "--out", in_csv
        ]
        rc = self._run_and_stream(cmd_ps, cwd=repo_root)
        if rc != 0 or self.stop_flag.is_set():
            return

        # ---------------------------------------------------------------------
        # モデルDIR（未指定なら models/<approach>/latest）
        # ---------------------------------------------------------------------
        if not model_dir:
            model_dir = os.path.join("models", approach, "latest")
        model_pkl = os.path.join(model_dir, "model.pkl")
        feat_pkl  = os.path.join(model_dir, "feature_pipeline.pkl")
        if not os.path.exists(model_pkl) or not os.path.exists(feat_pkl):
            self._log(f"ERROR: {approach} モデルが不足しています。\n  model: {model_pkl}\n  feature_pipeline: {feat_pkl}")
            return

        # ---------------------------------------------------------------------
        # predict_one_race（デバッグCSV出力は環境変数でON/OFF）
        # ---------------------------------------------------------------------
        cmd3 = [sys.executable, SCRIPTS["predict_one_race"],
                "--live-csv", in_csv,
                "--approach", approach,
                "--model", model_pkl,
                "--feature-pipeline", feat_pkl]
        if show_features:
            cmd3.append("--show-features")

        env3 = None
        if dump_debug:
            env3 = os.environ.copy()
            env3["ADAPTER_DUMP_CSV"] = os.path.join("data", "live", "_debug_merged.csv")
            env3["ADAPTER_DUMP_STEPS"] = "1"

        rc = self._run_and_stream(cmd3, cwd=repo_root, env=env3)
        if rc != 0 or self.stop_flag.is_set():
            return

        # 最終サマリ（運用ログ向け）
        self._log("------------------------------------------------------------")
        self._log(f"[SUMMARY] in_csv={in_csv}")
        self._log(f"[SUMMARY] approach={approach}")
        self._log(f"[SUMMARY] model_dir={model_dir}")
        if motor_enabled:
            self._log("[SUMMARY] motor=OK")
        else:
            self._log(f"[SUMMARY] motor=SKIPPED reason={motor_skip_reason}")
        self._log("------------------------------------------------------------")

        self._log("\n=== すべて完了しました ✅ ===\n")


# ====== GUI本体 ======
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1020x760")
        self.settings = load_settings()

        # 入力値
        self.var_date  = tk.StringVar(value=self.settings.get("date", today_jst_yyyymmdd()))
        self.var_jcd   = tk.StringVar(value=self.settings.get("jcd", "24"))
        self.var_race  = tk.StringVar(value=self.settings.get("race","12"))
        self.var_approach = tk.StringVar(value=self.settings.get("approach","base"))

        # 実行オプション（既存）
        self.var_use_csv        = tk.BooleanVar(value=self.settings.get("use_csv", False))
        self.var_csv_autoguess  = tk.BooleanVar(value=self.settings.get("csv_autoguess", True))
        self.var_csv_path       = tk.StringVar(value=self.settings.get("csv_path",""))
        self.var_show_features  = tk.BooleanVar(value=self.settings.get("show_features", False))
        self.var_dump_debug     = tk.BooleanVar(value=self.settings.get("dump_debug", False))

        # 詳細設定
        self.var_advanced = tk.BooleanVar(value=False)  # 起動時は表示OFF
        self.var_online   = tk.BooleanVar(value=self.settings.get("use_online", False))

        # アプローチごとにモデルDIRを保持
        self.var_model_dir  = tk.StringVar(value=self.settings.get("model_dir_base", os.path.join("models","base","latest")))
        self.var_model_dir_map = {
            "base": self.settings.get("model_dir_base", os.path.join("models","base","latest")),
            "sectional": self.settings.get("model_dir_sectional", os.path.join("models","sectional","latest")),
        }

        # ログ
        self.log_queue = queue.Queue()
        self.runner = Runner(self.log_queue)

        self._build_ui()
        self.after(50, self._poll_log_queue)

    def _build_ui(self):
        frm_in = ttk.LabelFrame(self, text="入力"); frm_in.pack(fill=tk.X, padx=10, pady=(10,6))
        ttk.Label(frm_in, text="日付(YYYYMMDD)").grid(row=0, column=0, sticky="w")
        ttk.Entry(frm_in, textvariable=self.var_date, width=12).grid(row=0, column=1, padx=(5,15))
        ttk.Label(frm_in, text="場コード").grid(row=0, column=2, sticky="w")
        ttk.Combobox(frm_in, textvariable=self.var_jcd, values=JCD_CHOICES, width=6, state="readonly").grid(row=0, column=3, padx=(5,15))
        ttk.Label(frm_in, text="レース").grid(row=0, column=4, sticky="w")
        ttk.Combobox(frm_in, textvariable=self.var_race, values=RACE_CHOICES, width=6, state="readonly").grid(row=0, column=5, padx=(5,15))
        ttk.Label(frm_in, text="アプローチ").grid(row=0, column=6, sticky="w")
        cmb = ttk.Combobox(frm_in, textvariable=self.var_approach, values=APPROACH_CHOICES, width=10, state="readonly")
        cmb.grid(row=0, column=7, padx=(5,15))
        cmb.bind("<<ComboboxSelected>>", self._on_change_approach)

        frm_btn = ttk.Frame(self); frm_btn.pack(fill=tk.X, padx=10, pady=6)
        self.btn_run  = ttk.Button(frm_btn, text="▶ 推論開始", command=self.on_run, width=20); self.btn_run.pack(side=tk.LEFT)
        self.btn_stop = ttk.Button(frm_btn, text="■ 停止", command=self.on_stop, width=10, state=tk.DISABLED); self.btn_stop.pack(side=tk.LEFT, padx=6)
        ttk.Checkbutton(frm_btn, text="既存CSVから推論", variable=self.var_use_csv).pack(side=tk.LEFT, padx=(18,8))
        ttk.Checkbutton(frm_btn, text="CSV自動推定", variable=self.var_csv_autoguess).pack(side=tk.LEFT, padx=(0,12))
        ttk.Checkbutton(frm_btn, text="列情報を表示 (--show-features)", variable=self.var_show_features).pack(side=tk.LEFT, padx=(0,12))
        ttk.Checkbutton(frm_btn, text="デバッグCSV出力 (_debug_merged.csv)", variable=self.var_dump_debug).pack(side=tk.LEFT, padx=(0,12))
        ttk.Button(frm_btn, text="📁 出力フォルダ（data/live）", command=self._open_live_dir).pack(side=tk.RIGHT)

        frm_csv = ttk.Frame(self); frm_csv.pack(fill=tk.X, padx=10, pady=(0,6))
        ttk.Label(frm_csv, text="CSVパス").pack(side=tk.LEFT)
        ttk.Entry(frm_csv, textvariable=self.var_csv_path).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)
        ttk.Button(frm_csv, text="参照", command=self._browse_csv).pack(side=tk.LEFT)

        self.frm_adv = ttk.LabelFrame(self, text="詳細設定（モデル/実験オプション）")
        ttk.Label(self.frm_adv, text="モデルディレクトリ（選択アプローチに対応）").grid(row=0, column=0, sticky="w")
        ttk.Entry(self.frm_adv, textvariable=self.var_model_dir).grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(self.frm_adv, text="参照", command=self._browse_model_dir).grid(row=0, column=2, padx=6)
        self.frm_adv.columnconfigure(1, weight=1)
        ttk.Checkbutton(self.frm_adv, text="build_live_row に --online（検証/非常時のみ。通常はOFF）",
                        variable=self.var_online).grid(row=1, column=0, columnspan=3, sticky="w", pady=(6,0))

        frm_toggle = ttk.Frame(self); frm_toggle.pack(fill=tk.X, padx=10, pady=(0,6))
        ttk.Checkbutton(frm_toggle, text="詳細設定を表示", variable=self.var_advanced, command=self._toggle_adv).pack(side=tk.LEFT)

        frm_log = ttk.LabelFrame(self, text="ログ"); frm_log.pack(fill=tk.BOTH, expand=True, padx=10, pady=(6,10))
        self.txt_log = tk.Text(frm_log, wrap="word", height=24)
        self.txt_log.pack(fill=tk.BOTH, expand=True)

    def _browse_csv(self):
        f = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if f:
            self.var_csv_path.set(f)

    def _browse_model_dir(self):
        d = filedialog.askdirectory(title=f"{self.var_approach.get()} モデルディレクトリ", initialdir=os.getcwd())
        if d:
            self.var_model_dir.set(d)
            self.var_model_dir_map[self.var_approach.get()] = d

    def _toggle_adv(self):
        if self.var_advanced.get():
            self.frm_adv.pack(fill=tk.X, padx=10, pady=(0,6))
        else:
            self.frm_adv.forget()

    def _open_live_dir(self):
        live_dir = os.path.join("data","live")
        Path(live_dir).mkdir(parents=True, exist_ok=True)
        if os.name == "nt":
            os.startfile(live_dir)
        elif sys.platform == "darwin":
            subprocess.Popen(["open", live_dir])
        else:
            subprocess.Popen(["xdg-open", live_dir])

    def _on_change_approach(self, _evt=None):
        ap = self.var_approach.get()
        d = self.var_model_dir_map.get(ap, os.path.join("models", ap, "latest"))
        self.var_model_dir.set(d)

    def on_run(self):
        date = self.var_date.get().strip()
        jcd  = self.var_jcd.get().strip()
        race = self.var_race.get().strip()
        approach = self.var_approach.get().strip()
        model_dir = self.var_model_dir.get().strip()

        use_csv       = bool(self.var_use_csv.get())
        csv_autoguess = bool(self.var_csv_autoguess.get())
        csv_path      = self.var_csv_path.get().strip()
        show_features = bool(self.var_show_features.get())
        use_online    = bool(self.var_online.get())
        dump_debug    = bool(self.var_dump_debug.get())

        if not valid_yyyymmdd(date):
            messagebox.showerror("入力エラー","日付は YYYYMMDD で入力してください。")
            return
        if jcd not in JCD_CHOICES:
            messagebox.showerror("入力エラー","場コードが不正です。")
            return
        try:
            r = int(race)
            assert 1 <= r <= 12
        except Exception:
            messagebox.showerror("入力エラー","レース番号は 1〜12 の整数で入力してください。")
            return
        if approach not in APPROACH_CHOICES:
            messagebox.showerror("入力エラー","アプローチが不正です。")
            return
        for p in SCRIPTS.values():
            if not os.path.exists(p):
                messagebox.showerror("ファイルなし", f"{p} が見つかりません。リポジトリ直下で実行してください。")
                return

        # 設定保存
        self.var_model_dir_map[approach] = model_dir
        save_settings({
            "date": date, "jcd": jcd, "race": race,
            "approach": approach,
            "use_csv": use_csv,
            "csv_autoguess": csv_autoguess,
            "csv_path": csv_path,
            "show_features": show_features,
            "dump_debug": dump_debug,
            "model_dir_base": self.var_model_dir_map.get("base", os.path.join("models","base","latest")),
            "model_dir_sectional": self.var_model_dir_map.get("sectional", os.path.join("models","sectional","latest")),
            "use_online": use_online,
        })

        # UIロック
        self.btn_run.config(state=tk.DISABLED)
        self.btn_stop.config(state=tk.NORMAL)
        self._log("="*76)
        self._log(f"開始: date={date}, jcd={jcd}, race={race}, approach={approach}")
        self._log(f"model_dir={model_dir or f'models/{approach}/latest'} | CSVモード={'ON' if use_csv else 'OFF'} | 自動推定={'ON' if csv_autoguess else 'OFF'} | show_features={'ON' if show_features else 'OFF'} | debug_csv={'ON' if dump_debug else 'OFF'} | online={'ON' if use_online else 'OFF'}")
        self._log("="*76)

        def _worker():
            try:
                self.runner.run_pipeline(
                    date_yyyymmdd=date, jcd=jcd, race=race,
                    approach=approach, model_dir=model_dir,
                    use_online=use_online,
                    use_csv=use_csv, csv_path=csv_path, csv_autoguess=csv_autoguess,
                    show_features=show_features,
                    repo_root=os.getcwd(),
                    dump_debug=dump_debug,
                )
            finally:
                self.btn_run.config(state=tk.NORMAL)
                self.btn_stop.config(state=tk.DISABLED)
                self._log("完了 / 停止")

        threading.Thread(target=_worker, daemon=True).start()

    def on_stop(self):
        self.runner.stop()
        self._log("停止要求を送信しました…")

    def _log(self, msg: str):
        t = datetime.now().strftime("%H:%M:%S")
        self.txt_log.insert(tk.END, f"[{t}] {msg}\n")
        self.txt_log.see(tk.END)

    def _poll_log_queue(self):
        try:
            while True:
                self.txt_log.insert(tk.END, self.log_queue.get_nowait() + "\n")
                self.txt_log.see(tk.END)
        except queue.Empty:
            pass
        finally:
            self.after(50, self._poll_log_queue)


if __name__ == "__main__":
    ensure_parent_dir(SETTINGS_FILE)
    app = App()
    app.mainloop()
