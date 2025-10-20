# gui_predict_one_race.py
# ------------------------------------------------------------
# 1レース推論 GUI（approach 切替 / スクレイプ or 既存CSV / CSV自動推定 / 列情報表示オプション）
# - フロー: scrape_one_race -> build_live_row -> predict_one_race
# - 既存CSVからの推論も可（raw_YYYYMMDD_JJ_R.csv など）
# - アプローチに連動した models/<approach>/latest を既定に使用
# - 「列情報を表示(--show-features)」はメインボタン群の近くに配置（デフォルトOFF）
# - 「CSV自動推定」はメインボタン群の近くに配置（on なら raw_{date}_{jcd}_{race}.csv を自動選択）
# - 設定は data/config/settings.json に保存/復元
# - 追加: 「デバッグCSV出力(_debug_merged.csv)」チェック（デフォルトOFF）
#         ON時のみ ADAPTER_DUMP_CSV / ADAPTER_DUMP_STEPS を predict サブプロセスへ付与
# ------------------------------------------------------------

import os
import sys
import re
import csv
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

# ====== 定数 ======
APP_TITLE = "Boatrace 1レース推論 GUI（base/sectional + CSV自動推定 + 列一覧オプション）"
SETTINGS_FILE = os.path.join("data", "config", "settings.json")
LEGACY_SETTINGS_FILE = "settings.json"  # 旧互換

JCD_CHOICES  = [f"{i:02d}" for i in range(1, 25)]
RACE_CHOICES = [f"{i}" for i in range(1, 13)]
APPROACH_CHOICES = ["base", "sectional"]

SCRIPTS = {
    "scrape_one_race": os.path.join("scripts", "scrape_one_race.py"),
    "build_live_row":  os.path.join("scripts", "build_live_row.py"),
    "predict_one_race": os.path.join("scripts", "predict_one_race.py"),
    "preprocess_course": os.path.join("scripts", "preprocess_course.py"),
    "preprocess_sectional": os.path.join("scripts", "preprocess_sectional.py"),
}

# ====== ユーティリティ ======
def ensure_parent_dir(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

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
    if not s or len(s) != 8 or not s.isdigit(): return False
    try:
        datetime.strptime(s, "%Y%m%d"); return True
    except ValueError:
        return False

# ====== 実行ランナー ======
class Runner:
    def __init__(self, log_queue: queue.Queue):
        self.log_queue = log_queue
        self.stop_flag = threading.Event()
        self.current_proc = None

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

    def _run_and_stream(self, cmd, cwd=None, env=None):
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
        """スクレイプ→推論 or CSV→推論 を実行"""
        # スクリプト存在チェック
        for k in ("scrape_one_race","build_live_row","predict_one_race"):
            if not os.path.exists(SCRIPTS[k]):
                self._log(f"ERROR: {SCRIPTS[k]} が見つかりません。リポジトリ直下で実行してください。")
                return

        # 1) 入力CSVを決める
        in_csv = None
        if use_csv:
            if csv_autoguess:
                # data/live/raw_{date}_{jcd}_{race}.csv を推定
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
            if rc != 0 or self.stop_flag.is_set(): return

            # 3) build_live_row
            out_csv = os.path.join("data","live", f"raw_{date_yyyymmdd}_{jcd}_{race}.csv")
            cmd2 = [sys.executable, SCRIPTS["build_live_row"],
                    "--date", date_yyyymmdd, "--jcd", jcd, "--race", race, "--out", out_csv]
            if use_online:
                cmd2.append("--online")
            rc = self._run_and_stream(cmd2, cwd=repo_root)
            if rc != 0 or self.stop_flag.is_set(): return
            in_csv = out_csv

        # 3.5) コース別履歴特徴を“上書き付与”して、live CSVに反映（学習と同ロジック）
        # - 除外前rawを end-date（=指定日）まで + warmup日ぶん遡って読み、shift→rolling(N)。
        # - entry は推論時、entry_tenji で補完（preprocess_course.py側で対応済み）。
        # - ライブ行ダミー追加により対象レース行にも履歴が付与される（分母には入れない）。
        if not in_csv:
            self._log("[ERROR] 内部エラー: in_csv が未確定です。"); return

        # 既定値（必要あればGUI拡張で可）
        DEFAULT_WARMUP_DAYS = 180
        DEFAULT_N_LAST = 10

        # YYYYMMDD → YYYY-MM-DD
        y, m, d = date_yyyymmdd[:4], date_yyyymmdd[4:6], date_yyyymmdd[6:]
        start_str = f"{y}-{m}-{d}"
        end_str   = f"{y}-{m}-{d}"

        # レポート出力先
        reports_dir = os.path.join("data", "processed", "course_meta_live")
        ensure_parent_dir(os.path.join(reports_dir, "_dummy.txt"))  # 親だけ確保

        cmd_pc = [
            sys.executable, SCRIPTS["preprocess_course"],
            "--master", in_csv,
            "--raw-dir", os.path.join("data", "raw"),
            "--out", in_csv,  # 上書き
            "--reports-dir", reports_dir,
            "--start-date", start_str,
            "--end-date",   end_str,
            "--warmup-days", str(DEFAULT_WARMUP_DAYS),
            "--n-last",       str(DEFAULT_N_LAST),
        ]
        
        rc = self._run_and_stream(cmd_pc, cwd=repo_root)
        if rc != 0 or self.stop_flag.is_set(): return

        # 3.6) 節間（sectional）列を live CSV に上書き付与（当日raceinfoが空でも必須列はNaNで保証）
        cmd_ps = [
            sys.executable,
            (SCRIPTS["preprocess_sectional"] if "SCRIPTS" in globals() and "preprocess_sectional" in SCRIPTS
             else os.path.join("scripts", "preprocess_sectional.py")),
            "--master", in_csv,
            "--raceinfo-dir", os.path.join("data", "processed", "raceinfo"),
            "--date", date_yyyymmdd,        # 単日
            "--live-html-root", os.path.join("data","live","html"),  # ← 追加
            "--out", in_csv                 # 同じCSVに上書き
        ]
        rc = self._run_and_stream(cmd_ps, cwd=repo_root)
        if rc != 0 or self.stop_flag.is_set():
            return

        # 4) モデルDIR（未指定なら models/<approach>/latest）
        if not model_dir:
            model_dir = os.path.join("models", approach, "latest")
        model_pkl = os.path.join(model_dir, "model.pkl")
        feat_pkl  = os.path.join(model_dir, "feature_pipeline.pkl")
        if not os.path.exists(model_pkl) or not os.path.exists(feat_pkl):
            self._log(f"ERROR: {approach} モデルが不足しています。\n  model: {model_pkl}\n  feature_pipeline: {feat_pkl}")
            return

        # 5) predict_one_race（デバッグCSV出力は環境変数でON/OFF）
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
            env3["ADAPTER_DUMP_STEPS"] = "1"  # 段階別に出す

        rc = self._run_and_stream(cmd3, cwd=repo_root, env=env3)
        if rc != 0 or self.stop_flag.is_set(): return

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

        # 実行オプション
        self.var_use_csv        = tk.BooleanVar(value=self.settings.get("use_csv", False))
        self.var_csv_autoguess  = tk.BooleanVar(value=self.settings.get("csv_autoguess", True))
        self.var_csv_path       = tk.StringVar(value=self.settings.get("csv_path",""))
        self.var_show_features  = tk.BooleanVar(value=self.settings.get("show_features", False))
        # 追加: デバッグCSV出力（デフォルトOFF）
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
        ttk.Label(frm_in, text="アプローチ").grid(row=0, column=6, sticky="w")
        cmb = ttk.Combobox(frm_in, textvariable=self.var_approach, values=APPROACH_CHOICES, width=10, state="readonly")
        cmb.grid(row=0, column=7, padx=(5,15))
        cmb.bind("<<ComboboxSelected>>", self._on_change_approach)

        # メインボタン群（ここに CSV/列情報/デバッグCSVチェックを配置）
        frm_btn = ttk.Frame(self); frm_btn.pack(fill=tk.X, padx=10, pady=6)
        self.btn_run  = ttk.Button(frm_btn, text="▶ 推論開始", command=self.on_run, width=20); self.btn_run.pack(side=tk.LEFT)
        self.btn_stop = ttk.Button(frm_btn, text="■ 停止", command=self.on_stop, width=10, state=tk.DISABLED); self.btn_stop.pack(side=tk.LEFT, padx=6)
        ttk.Checkbutton(frm_btn, text="既存CSVから推論", variable=self.var_use_csv).pack(side=tk.LEFT, padx=(18,8))
        ttk.Checkbutton(frm_btn, text="CSV自動推定", variable=self.var_csv_autoguess).pack(side=tk.LEFT, padx=(0,12))
        ttk.Checkbutton(frm_btn, text="列情報を表示 (--show-features)", variable=self.var_show_features).pack(side=tk.LEFT, padx=(0,12))
        # 追加: デバッグCSV出力（デフォルトOFF）
        ttk.Checkbutton(frm_btn, text="デバッグCSV出力 (_debug_merged.csv)", variable=self.var_dump_debug).pack(side=tk.LEFT, padx=(0,12))
        ttk.Button(frm_btn, text="📁 出力フォルダ（data/live）", command=self._open_live_dir).pack(side=tk.RIGHT)

        # CSV指定行
        frm_csv = ttk.Frame(self); frm_csv.pack(fill=tk.X, padx=10, pady=(0,6))
        ttk.Label(frm_csv, text="CSVパス").pack(side=tk.LEFT)
        ttk.Entry(frm_csv, textvariable=self.var_csv_path).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)
        ttk.Button(frm_csv, text="参照", command=self._browse_csv).pack(side=tk.LEFT)

        # 詳細設定（モデル/実験）
        self.frm_adv = ttk.LabelFrame(self, text="詳細設定（モデル/実験オプション）")
        ttk.Label(self.frm_adv, text="モデルディレクトリ（選択アプローチに対応）").grid(row=0, column=0, sticky="w")
        ttk.Entry(self.frm_adv, textvariable=self.var_model_dir).grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(self.frm_adv, text="参照", command=self._browse_model_dir).grid(row=0, column=2, padx=6)
        self.frm_adv.columnconfigure(1, weight=1)
        ttk.Checkbutton(self.frm_adv, text="build_live_row に --online（検証/非常時のみ。通常はOFF）",
                        variable=self.var_online).grid(row=1, column=0, columnspan=3, sticky="w", pady=(6,0))

        # 詳細設定表示トグル
        frm_toggle = ttk.Frame(self); frm_toggle.pack(fill=tk.X, padx=10, pady=(0,6))
        ttk.Checkbutton(frm_toggle, text="詳細設定を表示", variable=self.var_advanced, command=self._toggle_adv).pack(side=tk.LEFT)

        # ログ
        frm_log = ttk.LabelFrame(self, text="ログ"); frm_log.pack(fill=tk.BOTH, expand=True, padx=10, pady=(6,10))
        self.txt_log = tk.Text(frm_log, wrap="word", height=24)
        self.txt_log.pack(fill=tk.BOTH, expand=True)

    # ---------- UIハンドラ ----------
    def _browse_csv(self):
        f = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if f: self.var_csv_path.set(f)

    def _browse_model_dir(self):
        d = filedialog.askdirectory(title=f"{self.var_approach.get()} モデルディレクトリ", initialdir=os.getcwd())
        if d:
            self.var_model_dir.set(d)
            self.var_model_dir_map[self.var_approach.get()] = d  # アプローチごとに保持

    def _toggle_adv(self):
        if self.var_advanced.get(): self.frm_adv.pack(fill=tk.X, padx=10, pady=(0,6))
        else: self.frm_adv.forget()

    def _open_live_dir(self):
        live_dir = os.path.join("data","live"); Path(live_dir).mkdir(parents=True, exist_ok=True)
        if os.name == "nt": os.startfile(live_dir)
        elif sys.platform == "darwin": subprocess.Popen(["open", live_dir])
        else: subprocess.Popen(["xdg-open", live_dir])

    def _on_change_approach(self, _evt=None):
        ap = self.var_approach.get()
        d = self.var_model_dir_map.get(ap, os.path.join("models", ap, "latest"))
        self.var_model_dir.set(d)

    # ---------- 実行/停止 ----------
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

        # 入力チェック
        if not valid_yyyymmdd(date):
            messagebox.showerror("入力エラー","日付は YYYYMMDD で入力してください。"); return
        if jcd not in JCD_CHOICES:
            messagebox.showerror("入力エラー","場コードが不正です。"); return
        try:
            r = int(race); assert 1 <= r <= 12
        except Exception:
            messagebox.showerror("入力エラー","レース番号は 1〜12 の整数で入力してください。"); return
        if approach not in APPROACH_CHOICES:
            messagebox.showerror("入力エラー","アプローチが不正です。"); return
        for p in SCRIPTS.values():
            if not os.path.exists(p):
                messagebox.showerror("ファイルなし", f"{p} が見つかりません。リポジトリ直下で実行してください。"); return

        # 設定保存（アプローチごとにモデルDIRを保持）
        self.var_model_dir_map[approach] = model_dir
        save_settings({
            "date": date, "jcd": jcd, "race": race,
            "approach": approach,
            "use_csv": use_csv,
            "csv_autoguess": csv_autoguess,
            "csv_path": csv_path,
            "show_features": show_features,
            "dump_debug": dump_debug,  # ← 追加保存
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

        # 実行スレッド
        def _worker():
            try:
                self.runner.run_pipeline(
                    date_yyyymmdd=date, jcd=jcd, race=race,
                    approach=approach, model_dir=model_dir,
                    use_online=use_online,
                    use_csv=use_csv, csv_path=csv_path, csv_autoguess=csv_autoguess,
                    show_features=show_features,
                    repo_root=os.getcwd(),
                    dump_debug=dump_debug,  # ← 追加
                )
            finally:
                self.btn_run.config(state=tk.NORMAL)
                self.btn_stop.config(state=tk.DISABLED)
                self._log("完了 / 停止")

        threading.Thread(target=_worker, daemon=True).start()

    def on_stop(self):
        self.runner.stop()
        self._log("停止要求を送信しました…")

    # ---------- ログ中継 ----------
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


if __name__ == "__main__":
    ensure_parent_dir(SETTINGS_FILE)
    app = App()
    app.mainloop()
