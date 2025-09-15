# gui_predict_one_race.py
# ------------------------------------------------------------
# 1レース推論 GUI（自動フロー復元 + ability対応スジ舟券 / 出力分割）
#  - フロー: scrape_one_race -> build_live_row -> predict_one_race -> predict_top2pair
#  - レース入力: 場コードと同じ Combobox（1〜12）
#  - ログ/出力の2ペイン
#  - ability列は固定候補から選択（指定がCSVに無くても安全にスキップ）
#  - スジ舟券は scripts/suji_strategy.py（ability/逆転/閾値/graded対応）を使用
#  - 買い目候補（top2pair×base）も従来通り出力可能
#  - 設定保存先: data/config/settings.json
#  - 変更点: --online を「詳細設定」に移動（デフォルトOFF/非表示）
#           top2pair も base 同様「ディレクトリ指定」に統一
# ------------------------------------------------------------

import os, sys, re, csv, json, queue, signal, threading, subprocess, locale
from datetime import datetime, timezone, timedelta
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd

# === スジ舟券ロジック（ability対応） ===
from scripts.suji_strategy import generate_suji_tickets  # 最新版

APP_TITLE = "Boatrace 1レース推論 GUI（自動フロー + スジ舟券）"

# 設定ファイル
SETTINGS_FILE = os.path.join("data", "config", "settings.json")
LEGACY_SETTINGS_FILE = "settings.json"  # 旧版互換: あれば読み取り→新パスへ保存

# ability 候補（ユーザー指定リスト）
ABILITY_CHOICES = [
    "N_winning_rate",
    "N_2rentai_rate",
    "N_3rentai_rate",
    "LC_winning_rate",
    "LC_2rentai_rate",
    "LC_3rentai_rate",
    "motor_number",
    "motor_2rentai_rate",
    "motor_3rentai_rate",
    "time_tenji",
]

# 実行スクリプト
SCRIPTS = {
    "scrape_one_race": os.path.join("scripts", "scrape_one_race.py"),
    "build_live_row":  os.path.join("scripts", "build_live_row.py"),
    "predict_one_race": os.path.join("scripts", "predict_one_race.py"),
    "predict_top2pair": os.path.join("scripts", "predict_top2pair.py"),
}

JCD_CHOICES  = [f"{i:02d}" for i in range(1, 25)]
RACE_CHOICES = [f"{i}" for i in range(1, 13)]

# ---------- 設定の入出力 ----------
def ensure_parent_dir(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

def save_settings(state: dict):
    try:
        ensure_parent_dir(SETTINGS_FILE)
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def load_settings() -> dict:
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    # 旧版の ./settings.json を移行
    if os.path.exists(LEGACY_SETTINGS_FILE):
        try:
            with open(LEGACY_SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            save_settings(data)
            return data
        except Exception:
            return {}
    return {}

def today_jst_yyyymmdd() -> str:
    jst = timezone(timedelta(hours=9))
    return datetime.now(jst).strftime("%Y%m%d")

def valid_yyyymmdd(s: str) -> bool:
    if not s or len(s) != 8 or not s.isdigit(): return False
    try:
        datetime.strptime(s, "%Y%m%d"); return True
    except ValueError:
        return False

# ---------- ability 構築 ----------
def build_ability_from_csv(csv_path: str, ability_col: str, logger=None):
    """CSVから ability_col を 0..1 正規化して {wakuban: score} を返す。無ければ None。"""
    if not ability_col: return None
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        if logger: logger(f"[WARN] ability: CSV読み込み失敗: {e}")
        return None

    wcol = next((c for c in ["wakuban","枠番","lane","lane_no","WAKUBAN"] if c in df.columns), None)
    if wcol is None:
        if len(df) >= 6:
            df = df.head(6).copy()
            df["__wakuban__"] = list(range(1,7))
            wcol = "__wakuban__"
        else:
            if logger: logger("[WARN] ability: wakuban 列が見つからず")
            return None

    if ability_col not in df.columns:
        if logger: logger(f"[WARN] ability: 指定列が見つかりません -> {ability_col}")
        return None

    sub = df[[wcol, ability_col]].dropna().copy()
    sub[ability_col] = pd.to_numeric(sub[ability_col], errors="coerce")
    sub = sub.dropna()
    if sub.empty:
        if logger: logger(f"[INFO] ability: 値が空のためスキップ（{ability_col}）")
        return None

    vmax = sub[ability_col].max()
    if vmax <= 0: return None

    sub["__ab__"] = sub[ability_col] / vmax
    ability = {int(w): float(a) for w, a in zip(sub[wcol], sub["__ab__"]) if 1 <= int(w) <= 6}
    for k in range(1,7): ability.setdefault(k, 0.0)

    if logger: logger(f"[OK] ability 構築: 列='{ability_col}' -> {ability}")
    return ability

# ---------- ランナー（自動フロー＋ログ中継） ----------
class Runner:
    def __init__(self, log_queue: queue.Queue):
        self.log_queue = log_queue
        self.stop_flag = threading.Event()
        self.current_proc = None
        self.last_live_csv = None
        self.last_top2_csv = None
        self.base_probs = {}     # {1..6: float}
        self.last_race_id = None

    def _log(self, text: str):
        self.log_queue.put(text)

    def stop(self):
        self.stop_flag.set()
        try:
            if self.current_proc and self.current_proc.poll() is None:
                if os.name == "nt":
                    self.current_proc.send_signal(signal.CTRL_BREAK_EVENT)
                self.current_proc.terminate()
        except Exception:
            pass

    def _run_and_stream(self, cmd, cwd=None, capture_base=False):
        if self.stop_flag.is_set(): return 1
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
            creationflags=creationflags, preexec_fn=preexec_fn
        )

        in_summary, skip_header = False, False
        for raw in self.current_proc.stdout:
            line = (raw or "").rstrip("\n")
            self._log(line)

            if capture_base:
                if line.strip().startswith("[SUMMARY]") and "prob" in line:
                    in_summary = True; skip_header = True; continue
                if in_summary:
                    if not line.strip():
                        in_summary = False; continue
                    if skip_header:
                        skip_header = False; continue
                    toks = line.split()
                    if len(toks) >= 6:
                        try:
                            wakuban = int(toks[3]); proba = float(toks[-1])
                            if 1 <= wakuban <= 6:
                                self.base_probs[wakuban] = proba
                        except Exception:
                            pass

        rc = self.current_proc.wait()
        self.current_proc = None
        self._log(f"[exit code] {rc}\n")
        return rc

    def run_pipeline(self, date_yyyymmdd: str, jcd: str, race: str,
                     model_dir_base: str, use_online: bool, also_top2pair: bool,
                     repo_root: str, adv_enabled: bool, top2_model_dir: str | None):
        """旧GUIの自動フローを完全復元し、base確率のパースも行う。"""
        self.last_live_csv = None
        self.last_top2_csv = None
        self.base_probs = {}
        self.last_race_id = f"{date_yyyymmdd}{jcd}{int(race):02d}"

        # スクリプト存在チェック
        for k in ("scrape_one_race","build_live_row","predict_one_race"):
            if not os.path.exists(SCRIPTS[k]):
                self._log(f"ERROR: {SCRIPTS[k]} が見つかりません。リポジトリ直下で実行してください。")
                return

        # base モデル
        if not model_dir_base:
            model_dir_base = os.path.join("models","base","latest")
        model_pkl = os.path.join(model_dir_base, "model.pkl")
        feat_pkl  = os.path.join(model_dir_base, "feature_pipeline.pkl")
        if not os.path.exists(model_pkl) or not os.path.exists(feat_pkl):
            self._log(f"ERROR: base モデルが不足しています。\n  model: {model_pkl}\n  feature_pipeline: {feat_pkl}")
            return

        try:
            # 1) scrape
            rc = self._run_and_stream(
                [sys.executable, SCRIPTS["scrape_one_race"], "--date", date_yyyymmdd, "--jcd", jcd, "--race", race],
                cwd=repo_root
            )
            if rc != 0 or self.stop_flag.is_set(): return

            # 2) build_live_row
            out_csv = os.path.join("data","live", f"raw_{date_yyyymmdd}_{jcd}_{race}.csv")
            cmd2 = [sys.executable, SCRIPTS["build_live_row"],
                    "--date", date_yyyymmdd, "--jcd", jcd, "--race", race, "--out", out_csv]
            if use_online:
                cmd2.append("--online")  # 高度設定でONのときだけ
            rc = self._run_and_stream(cmd2, cwd=repo_root)
            if rc != 0 or self.stop_flag.is_set(): return
            self.last_live_csv = out_csv

            # 3) base predict（確率パースON）
            rc = self._run_and_stream(
                [sys.executable, SCRIPTS["predict_one_race"], "--live-csv", out_csv,
                 "--model", model_pkl, "--feature-pipeline", feat_pkl],
                cwd=repo_root, capture_base=True
            )
            if rc != 0 or self.stop_flag.is_set(): return

            # 4) top2pair（任意）
            if also_top2pair and os.path.exists(SCRIPTS["predict_top2pair"]):
                race_id = f"{date_yyyymmdd}{jcd}{int(race):02d}"
                cmd4 = [sys.executable, SCRIPTS["predict_top2pair"],
                        "--mode", "live", "--master", out_csv, "--race-id", race_id]
                # 詳細設定で top2pair ディレクトリ指定があれば優先
                top2_dir = None
                if adv_enabled and top2_model_dir and os.path.isdir(top2_model_dir):
                    mp = os.path.join(top2_model_dir, "model.pkl")
                    if os.path.exists(mp):
                        cmd4 += ["--model", mp]
                        top2_dir = top2_model_dir
                    else:
                        self._log(f"[WARN] top2pair: {top2_model_dir} に model.pkl が見つかりません。latest を使用します。")
                rc = self._run_and_stream(cmd4, cwd=repo_root)
                if rc != 0 or self.stop_flag.is_set(): return
                tcsv = os.path.join("data","live","top2pair", f"pred_{date_yyyymmdd}_{jcd}_{race}.csv")
                if os.path.exists(tcsv): self.last_top2_csv = tcsv

            self._log("\n=== すべて完了しました ✅ ===\n")

        except Exception as e:
            self._log(f"\n[例外] {e}\n")

# ---------- GUI ----------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1000x800")
        self.settings = load_settings()

        # 入力値
        self.var_date  = tk.StringVar(value=self.settings.get("date", today_jst_yyyymmdd()))
        self.var_jcd   = tk.StringVar(value=self.settings.get("jcd", "24"))
        self.var_race  = tk.StringVar(value=self.settings.get("race","12"))
        self.var_top2   = tk.BooleanVar(value=self.settings.get("also_top2pair", True))
        self.var_advanced = tk.BooleanVar(value=False)  # 起動時は常にOFF

        # 詳細（モデル/実験）
        self.var_model_dir  = tk.StringVar(value=self.settings.get("model_dir", os.path.join("models","base","latest")))
        self.var_top2_dir   = tk.StringVar(value=self.settings.get("top2_dir",  os.path.join("models","top2pair","latest")))
        self.var_online     = tk.BooleanVar(value=self.settings.get("use_online", False))  # 詳細に移動（通常OFF）

        # スジ舟券（高度設定）
        self.var_grade   = tk.StringVar(value=self.settings.get("grade","normal"))
        self.var_use_ab  = tk.BooleanVar(value=self.settings.get("use_ability", True))
        self.var_ab_col  = tk.StringVar(value=self.settings.get("ability_col","N_winning_rate"))
        self.var_topn    = tk.IntVar(value=int(self.settings.get("suji_topn_per_attacker", 10)))
        self.var_attmax  = tk.IntVar(value=int(self.settings.get("suji_attackers_max", 1)))   # 既定 1
        self.var_thresh  = tk.DoubleVar(value=float(self.settings.get("suji_threshold", 0.20)))

        # 実行系
        self.log_queue = queue.Queue()
        self.runner = Runner(self.log_queue)
        self.worker_thread = None

        self._build_ui()
        self.after(50, self._poll_log_queue)

    def _build_ui(self):
        # 入力
        frm_in = ttk.LabelFrame(self, text="入力"); frm_in.pack(fill=tk.X, padx=10, pady=(10,6))
        ttk.Label(frm_in, text="日付(YYYYMMDD)").grid(row=0, column=0, sticky="w")
        ttk.Entry(frm_in, textvariable=self.var_date, width=12).grid(row=0, column=1, padx=(5,15))
        ttk.Label(frm_in, text="場コード").grid(row=0, column=2, sticky="w")
        ttk.Combobox(frm_in, textvariable=self.var_jcd, values=JCD_CHOICES, width=6, state="readonly").grid(row=0, column=3, padx=(5,15))
        ttk.Label(frm_in, text="レース").grid(row=0, column=4, sticky="w")
        ttk.Combobox(frm_in, textvariable=self.var_race, values=RACE_CHOICES, width=6, state="readonly").grid(row=0, column=5, padx=(5,15))

        # オプション（簡素化）
        frm_btn = ttk.Frame(self); frm_btn.pack(fill=tk.X, padx=10, pady=6)
        self.btn_run  = ttk.Button(frm_btn, text="▶ 推論開始", command=self.on_run, width=20); self.btn_run.pack(side=tk.LEFT)
        self.btn_stop = ttk.Button(frm_btn, text="■ 停止", command=self.on_stop, width=10, state=tk.DISABLED); self.btn_stop.pack(side=tk.LEFT, padx=6)
        ttk.Checkbutton(frm_btn, text="top2pair も同時に推論", variable=self.var_top2).pack(side=tk.LEFT, padx=12)
        ttk.Checkbutton(frm_btn, text="詳細設定（モデル/実験）", variable=self.var_advanced, command=self._toggle_adv).pack(side=tk.LEFT, padx=12)
        ttk.Button(frm_btn, text="📁 出力フォルダ（data/live）", command=self._open_live_dir).pack(side=tk.RIGHT)

        # 詳細設定（モデル/実験）
        self.frm_adv = ttk.LabelFrame(self, text="詳細設定（モデル/実験オプション）")
        ttk.Label(self.frm_adv, text="base モデルディレクトリ").grid(row=0, column=0, sticky="w")
        ttk.Entry(self.frm_adv, textvariable=self.var_model_dir).grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(self.frm_adv, text="参照", command=self._browse_model_base).grid(row=0, column=2, padx=6)
        self.frm_adv.columnconfigure(1, weight=1)

        ttk.Label(self.frm_adv, text="top2pair モデルディレクトリ").grid(row=1, column=0, sticky="w")
        ttk.Entry(self.frm_adv, textvariable=self.var_top2_dir).grid(row=1, column=1, sticky="we", padx=6)
        ttk.Button(self.frm_adv, text="参照", command=self._browse_model_top2_dir).grid(row=1, column=2, padx=6)

        ttk.Checkbutton(self.frm_adv, text="build_live_row に --online（検証/非常時のみ。通常はOFF）",
                        variable=self.var_online).grid(row=2, column=0, columnspan=3, sticky="w", pady=(6,0))

        if self.var_advanced.get(): self.frm_adv.pack(fill=tk.X, padx=10, pady=(0,6))

        # 高度設定（スジ舟券）
        frm_suji = ttk.LabelFrame(self, text="高度設定（スジ舟券）"); frm_suji.pack(fill=tk.X, padx=10, pady=(0,6))
        ttk.Label(frm_suji, text="レース格").grid(row=0, column=0, sticky="e")
        ttk.Combobox(frm_suji, textvariable=self.var_grade, values=["normal","graded"], width=10, state="readonly").grid(row=0, column=1, sticky="w", padx=6)
        ttk.Checkbutton(frm_suji, text="ability（下の列名を使用）", variable=self.var_use_ab).grid(row=0, column=2, sticky="w", padx=10)
        ttk.Label(frm_suji, text="ability列名").grid(row=0, column=3, sticky="e")
        ttk.Combobox(frm_suji, textvariable=self.var_ab_col, values=ABILITY_CHOICES, width=22).grid(row=0, column=4, sticky="w", padx=6)

        ttk.Label(frm_suji, text="top_n/攻め艇").grid(row=1, column=0, sticky="e")
        ttk.Spinbox(frm_suji, from_=6, to=30, increment=1, textvariable=self.var_topn, width=6).grid(row=1, column=1, sticky="w")
        ttk.Label(frm_suji, text="攻め艇 最大").grid(row=1, column=2, sticky="e")
        ttk.Spinbox(frm_suji, from_=1, to=3, increment=1, textvariable=self.var_attmax, width=6).grid(row=1, column=3, sticky="w")
        ttk.Label(frm_suji, text="攻めスコア閾値").grid(row=1, column=4, sticky="e")
        ttk.Spinbox(frm_suji, from_=0.05, to=0.50, increment=0.01, textvariable=self.var_thresh, width=6).grid(row=1, column=5, sticky="w")

        # ログ + 出力
        frm_log = ttk.LabelFrame(self, text="ログ"); frm_log.pack(fill=tk.BOTH, expand=True, padx=10, pady=(6,4))
        self.txt_log = tk.Text(frm_log, wrap="word", height=16); self.txt_log.pack(fill=tk.BOTH, expand=True)

        frm_out = ttk.LabelFrame(self, text="出力"); frm_out.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0,10))
        self.txt_out = tk.Text(frm_out, wrap="word", height=14); self.txt_out.pack(fill=tk.BOTH, expand=True)

        # 右下：スジ/買い目
        frm_action = ttk.Frame(self); frm_action.pack(fill=tk.X, padx=10, pady=(0,8))
        self.btn_buy  = ttk.Button(frm_action, text="🧮 買い目候補 生成", command=self.on_generate_buy, state=tk.DISABLED)
        self.btn_buy.pack(side=tk.RIGHT, padx=8)
        self.btn_suji = ttk.Button(frm_action, text="🎯 スジ舟券 生成", command=self.on_generate_suji, state=tk.DISABLED)
        self.btn_suji.pack(side=tk.RIGHT, padx=8)

    # ---------- ユーティリティ ----------
    def _log(self, msg: str):
        t = datetime.now().strftime("%H:%M:%S")
        self.txt_log.insert(tk.END, f"[{t}] {msg}\n"); self.txt_log.see(tk.END)

    def _poll_log_queue(self):
        try:
            while True:
                self.txt_log.insert(tk.END, self.log_queue.get_nowait() + "\n")
                self.txt_log.see(tk.END)
        except queue.Empty:
            pass
        finally:
            self.after(50, self._poll_log_queue)

    def _toggle_adv(self):
        if self.var_advanced.get(): self.frm_adv.pack(fill=tk.X, padx=10, pady=(0,6))
        else: self.frm_adv.forget()

    def _browse_model_base(self):
        d = filedialog.askdirectory(title="base モデルディレクトリ", initialdir=os.getcwd())
        if d: self.var_model_dir.set(d)

    def _browse_model_top2_dir(self):
        d = filedialog.askdirectory(title="top2pair モデルディレクトリ", initialdir=os.getcwd())
        if d: self.var_top2_dir.set(d)

    def _open_live_dir(self):
        live_dir = os.path.join("data","live"); Path(live_dir).mkdir(parents=True, exist_ok=True)
        if os.name == "nt": os.startfile(live_dir)
        elif sys.platform == "darwin": subprocess.Popen(["open", live_dir])
        else: subprocess.Popen(["xdg-open", live_dir])

    # ---------- 実行/停止 ----------
    def on_run(self):
        date = self.var_date.get().strip()
        jcd  = self.var_jcd.get().strip()
        race = self.var_race.get().strip()

        if not valid_yyyymmdd(date):
            messagebox.showerror("入力エラー","日付は YYYYMMDD で入力してください。"); return
        if jcd not in JCD_CHOICES:
            messagebox.showerror("入力エラー","場コードが不正です。"); return
        try:
            r = int(race); assert 1 <= r <= 12
        except Exception:
            messagebox.showerror("入力エラー","レース番号は 1〜12 の整数で入力してください。"); return
        for p in SCRIPTS.values():
            if not os.path.exists(p):
                messagebox.showerror("ファイルなし", f"{p} が見つかりません。"); return

        # 設定保存
        save_settings({
            "date": date, "jcd": jcd, "race": race,
            "also_top2pair": bool(self.var_top2.get()),
            "model_dir": self.var_model_dir.get().strip(),
            "top2_dir": self.var_top2_dir.get().strip(),
            "use_online": bool(self.var_online.get()),
            # suji
            "grade": self.var_grade.get(),
            "use_ability": bool(self.var_use_ab.get()),
            "ability_col": self.var_ab_col.get(),
            "suji_topn_per_attacker": int(self.var_topn.get()),
            "suji_attackers_max": int(self.var_attmax.get()),
            "suji_threshold": float(self.var_thresh.get()),
        })

        # UIロック
        self.btn_buy.config(state=tk.DISABLED)
        self.btn_suji.config(state=tk.DISABLED)

        self._log("="*70)
        self._log(f"開始: date={date}, jcd={jcd}, race={race}")
        self._log(f"base_dir={self.var_model_dir.get() or 'models/base/latest'}, "
                  f"top2_dir={self.var_top2_dir.get() or 'models/top2pair/latest'}, "
                  f"online={'ON' if (self.var_advanced.get() and self.var_online.get()) else 'OFF'}")
        self._log("="*70)

        # フロー起動
        self.runner.stop_flag.clear()
        self.worker_thread = threading.Thread(
            target=self.runner.run_pipeline,
            args=(date, jcd, race,
                  self.var_model_dir.get().strip(),
                  bool(self.var_advanced.get() and self.var_online.get()),  # 詳細ON時のみ反映
                  bool(self.var_top2.get()),
                  os.getcwd(),
                  bool(self.var_advanced.get()),
                  self.var_top2_dir.get().strip()),
            daemon=True
        )
        self.worker_thread.start()
        self.after(500, self._check_done)

    def _check_done(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self.after(500, self._check_done); return
        # 完了
        if self.runner.base_probs: self.btn_suji.config(state=tk.NORMAL)
        if self.runner.last_top2_csv: self.btn_buy.config(state=tk.NORMAL)
        self._log("完了 / 停止")

    def on_stop(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self.runner.stop()
            self._log("停止要求を送信しました…")

    # ---------- 出力系（スジ/買い目） ----------
    def on_generate_suji(self):
        if not self.runner.base_probs:
            messagebox.showwarning("データ不足","base の確率が未取得です。先に推論を実行してください。")
            return

        csv_path = self.runner.last_live_csv
        ability = None
        if self.var_use_ab.get():
            ability = build_ability_from_csv(csv_path, self.var_ab_col.get(), logger=self._log)

        try:
            suji_list = generate_suji_tickets(
                base_probs=self.runner.base_probs,
                pairs=[],  # 未使用（互換）
                top_n_per_attacker=int(self.var_topn.get()),
                attackers_max=int(self.var_attmax.get()),    # 既定 1
                threshold=float(self.var_thresh.get()),
                event_grade=self.var_grade.get(),
                ability=ability,
            )
        except Exception as e:
            messagebox.showerror("エラー", f"スジ舟券生成でエラー:\n{e}")
            return

        self.txt_out.insert("end", "=== 🧭 スジ舟券候補 ===\n")
        if not suji_list:
            self.txt_out.insert("end","（候補なし：1逃げが強すぎ / 攻め艇が閾値未満）\n")
        else:
            for key, sc, tag in suji_list:
                self.txt_out.insert("end", f"{key}\tscore={sc:.6f}\t{tag}\n")
        self.txt_out.insert("end","\n")

    def on_generate_buy(self):
        top2_csv = self.runner.last_top2_csv
        base_probs = self.runner.base_probs.copy()
        race_id = self.runner.last_race_id
        if not (top2_csv and os.path.exists(top2_csv)):
            messagebox.showwarning("データ不足","top2pair の結果CSVが見つかりません。先に推論を実行してください。")
            return
        pairs = self._read_top2_pairs(top2_csv)
        if not pairs:
            pairs = self._read_top2_pairs_from_log(self.txt_log.get("1.0","end"))
        if not pairs:
            messagebox.showwarning("データ不足","top2pair のペアを取得できませんでした（CSV/ログともに読めず）。")
            return

        # 2連単
        tickets_2 = []
        for (i,j,p) in pairs:
            bi, bj = base_probs.get(i,1e-6), base_probs.get(j,1e-6)
            head, tail, hp, tp = (i,j,bi,bj) if bi >= bj else (j,i,bj,bi)
            score = p * (0.6*hp + 0.4*tp)
            tickets_2.append((f"{head}-{tail}", score))
        tickets_2.sort(key=lambda x: x[1], reverse=True)
        top2rentan = tickets_2[:10]

        # 3連複（簡易）
        tickets_3 = []
        for (i,j,p) in pairs[:30]:
            third, best = None, -1.0
            for k in range(1,7):
                if k in (i,j): continue
                bk = base_probs.get(k,1e-6)
                if bk > best: best, third = bk, k
            if third is None: continue
            trio = tuple(sorted([i,j,third]))
            score = p * (0.5*max(base_probs.get(i,0), base_probs.get(j,0)) + 0.5*best)
            tickets_3.append(("-".join(map(str,trio)), score))
        agg = {}
        for fmt,sc in tickets_3: agg[fmt] = max(agg.get(fmt,0.0), sc)
        top3 = sorted(agg.items(), key=lambda x:x[1], reverse=True)[:10]

        # 出力
        self.txt_out.insert("end", "=== 🎯買い目候補（暫定スコア） ===\n")
        if race_id: self.txt_out.insert("end", f"race_id: {race_id}\n\n")
        self.txt_out.insert("end", "[2連単 TOP10]\n")
        for fmt, sc in top2rentan: self.txt_out.insert("end", f"{fmt}\t score={sc:.6f}\n")
        self.txt_out.insert("end", "\n[3連複 TOP10]\n")
        for fmt, sc in top3: self.txt_out.insert("end", f"{fmt}\t score={sc:.6f}\n")
        self.txt_out.insert("end", "\n※ スコアは base(頭/相手の確率) と top2pair の p_top2set を掛け合わせた暫定指標。直前オッズと組み合わせてEV/ケリー配分に落とすのがおすすめ。\n\n")

    # ---------- CSV/ログ パース（top2pair） ----------
    def _read_top2_pairs(self, csv_path: str):
        try:
            with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
                sample = f.read(2048); f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",\t; ")
                except Exception:
                    class _D: delimiter = ","
                    dialect = _D()
                reader = csv.DictReader(f, dialect=dialect, skipinitialspace=True)
                if not reader.fieldnames: return []
                norm = {(c or "").strip().lower(): c for c in reader.fieldnames}
                def pick(row, keys):
                    for k in keys:
                        c = norm.get(k)
                        if c is not None and row.get(c, "") != "": return row[c]
                    for c in row.keys():
                        if (c or "").strip().lower() in keys and row.get(c, "") != "": return row[c]
                    return None
                pairs = []
                for row in reader:
                    si = pick(row, {"i"}); sj = pick(row, {"j"})
                    sp = pick(row, {"p_top2set","ptop2set","prob","p"})
                    try:
                        i = int(str(si).strip()); j = int(str(sj).strip()); p = float(str(sp).strip())
                        if 1 <= i <= 6 and 1 <= j <= 6 and i != j:
                            pairs.append((i, j, p))
                    except Exception:
                        continue
                return pairs
        except Exception:
            # フォールバック（簡易）
            try:
                pairs = []
                with open(csv_path, "r", encoding="utf-8-sig") as f:
                    header = f.readline()
                    for line in f:
                        cols = [c.strip() for c in re.split(r"[,\t]+", line.strip())]
                        if len(cols) >= 4:
                            i, j, p = int(cols[1]), int(cols[2]), float(cols[3])
                            if 1 <= i <= 6 and 1 <= j <= 6 and i != j:
                                pairs.append((i, j, p))
                return pairs
            except Exception:
                return []

    def _read_top2_pairs_from_log(self, log_text: str):
        pairs, in_block = [], False
        for line in log_text.splitlines():
            if "[TOP10 pairs by p_top2set]" in line:
                in_block = True; continue
            if in_block:
                if not line.strip():
                    if pairs: break
                    else: continue
                if re.search(r"\brace_id\b", line) and re.search(r"\bp_top2set\b", line):
                    continue
                toks = line.strip().split()
                try:
                    p = float(toks[-1]); j = int(toks[-2]); i = int(toks[-3])
                    if 1 <= i <= 6 and 1 <= j <= 6 and i != j:
                        pairs.append((i, j, p))
                except Exception:
                    continue
        return pairs

if __name__ == "__main__":
    ensure_parent_dir(SETTINGS_FILE)
    app = App()
    app.mainloop()
