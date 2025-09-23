# gui_predict_one_race.py
# ------------------------------------------------------------
# 1レース推論GUI（シンプル版 / ログ保持 + SUMMARYにモデル情報 + CSV自動推定）
#  - アプローチ切替（base / sectional）
#  - 2フロー:
#     (A) scrape_one_race -> build_live_row -> predict_one_race
#     (B) 既存CSVから predict_one_race（ファイル自動推定あり）
#  - モデルDIRは選択アプローチに連動（未指定時は models/<approach>/latest）
#  - 設定保存: data/config/settings.json（旧 settings.json から移行）
#  - ログは自動クリアしない。🧹ボタンで手動クリア。
#  - [SUMMARY] の行に "(approach=..., model_dir=...)" をインライン追記。
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

APP_TITLE = "Boatrace 1レース推論 GUI（シンプル版）"

# 設定ファイル
SETTINGS_FILE = os.path.join("data", "config", "settings.json")
LEGACY_SETTINGS_FILE = "settings.json"  # 旧互換

# 実行スクリプト
SCRIPTS = {
    "scrape_one_race": os.path.join("scripts", "scrape_one_race.py"),
    "build_live_row":  os.path.join("scripts", "build_live_row.py"),
    "predict_one_race": os.path.join("scripts", "predict_one_race.py"),
}

JCD_CHOICES  = [f"{i:02d}" for i in range(1, 25)]
RACE_CHOICES = [f"{i}" for i in range(1, 13)]
APPROACH_CHOICES = ["base", "sectional"]

# ----------------- 設定 I/O -----------------
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
    if not s or len(s) != 8 or not s.isdigit():
        return False
    try:
        datetime.strptime(s, "%Y%m%d")
        return True
    except ValueError:
        return False

# ----------------- 実行ランナー -----------------
class Runner:
    def __init__(self, log_queue: queue.Queue):
        self.log_queue = log_queue
        self.stop_flag = threading.Event()
        self.current_proc = None
        self.last_live_csv = None

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

    def _run_and_stream(self, cmd, cwd=None, transform=None):
        """
        子プロセスの標準出力を逐次中継。
        transform(line) が与えられた場合は、ログ出力前に行を置換できます。
        """
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
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            encoding=enc,
            errors="replace",
            creationflags=creationflags,
            preexec_fn=preexec_fn
        )

        for raw in self.current_proc.stdout:
            line = (raw or "").rstrip("\n")
            if transform:
                try:
                    line = transform(line)
                except Exception:
                    pass
            self._log(line)

        rc = self.current_proc.wait()
        self.current_proc = None
        self._log(f"[exit code] {rc}\n")
        return rc

    def _resolve_model_dir(self, approach: str, model_dir: str | None) -> str:
        if model_dir and model_dir.strip():
            return model_dir.strip()
        return os.path.join("models", approach, "latest")

    def _make_summary_inliner(self, approach: str, mdl_dir: str):
        """
        [SUMMARY] 行を検知したら、モデル情報をその行の末尾に追記するtransform関数を返す。
        """
        def _tf(line: str) -> str:
            s = line.strip()
            # 先頭が "[SUMMARY] prob(" で始まる行だけにインライン追記
            if s.startswith("[SUMMARY] prob("):
                # 末尾に " (approach=..., model_dir=...)" を追加
                return f"{line} (approach={approach}, model_dir={mdl_dir})"
            return line
        return _tf

    def _run_predict(self, csv_path: str, approach: str, mdl_dir: str, repo_root: str):
        model_pkl = os.path.join(mdl_dir, "model.pkl")
        feat_pkl  = os.path.join(mdl_dir, "feature_pipeline.pkl")
        if not os.path.exists(model_pkl) or not os.path.exists(feat_pkl):
            self._log(f"ERROR: {approach} モデル不足:\n  {model_pkl}\n  {feat_pkl}")
            return 2
        # 推論（[SUMMARY]行にモデル情報をインライン追記）
        cmd = [sys.executable, SCRIPTS["predict_one_race"],
               "--live-csv", csv_path,
               "--approach", approach,
               "--model", model_pkl,
               "--feature-pipeline", feat_pkl]
        return self._run_and_stream(cmd, cwd=repo_root,
                                    transform=self._make_summary_inliner(approach, mdl_dir))

    def run_from_scrape(self, date_yyyymmdd: str, jcd: str, race: str,
                        approach: str, model_dir: str | None,
                        use_online: bool, repo_root: str):
        """(A) スクレイプ→生成→推論"""
        self.last_live_csv = None

        # スクリプト存在チェック
        for k in ("scrape_one_race","build_live_row","predict_one_race"):
            if not os.path.exists(SCRIPTS[k]):
                self._log(f"ERROR: {SCRIPTS[k]} が見つかりません。リポジトリ直下で実行してください。")
                return

        mdl_dir = self._resolve_model_dir(approach, model_dir)

        try:
            # 1) scrape
            rc = self._run_and_stream(
                [sys.executable, SCRIPTS["scrape_one_race"], "--date", date_yyyymmdd, "--jcd", jcd, "--race", race],
                cwd=repo_root
            )
            if rc != 0 or self.stop_flag.is_set():
                return

            # 2) build_live_row
            out_csv = os.path.join("data","live", f"raw_{date_yyyymmdd}_{jcd}_{race}.csv")
            cmd2 = [sys.executable, SCRIPTS["build_live_row"],
                    "--date", date_yyyymmdd, "--jcd", jcd, "--race", race, "--out", out_csv]
            if use_online:
                cmd2.append("--online")
            rc = self._run_and_stream(cmd2, cwd=repo_root)
            if rc != 0 or self.stop_flag.is_set():
                return
            self.last_live_csv = out_csv

            # 3) predict_one_race
            rc = self._run_predict(out_csv, approach, mdl_dir, repo_root)
            if rc != 0 or self.stop_flag.is_set():
                return

            self._log("\n=== 完了 ✅ （スクレイプ→推論）===\n")

        except Exception as e:
            self._log(f"\n[例外] {e}\n")

    def run_from_csv(self, csv_path: str, approach: str, model_dir: str | None, repo_root: str):
        """(B) 既存CSV→推論"""
        self.last_live_csv = None

        if not os.path.exists(SCRIPTS["predict_one_race"]):
            self._log(f"ERROR: {SCRIPTS['predict_one_race']} が見つかりません。")
            return
        if not os.path.exists(csv_path):
            self._log(f"ERROR: 入力CSVが見つかりません: {csv_path}")
            return

        mdl_dir = self._resolve_model_dir(approach, model_dir)

        try:
            rc = self._run_predict(csv_path, approach, mdl_dir, repo_root)
            if rc != 0 or self.stop_flag.is_set():
                return

            self.last_live_csv = csv_path
            self._log("\n=== 完了 ✅（既存CSV→推論）===\n")

        except Exception as e:
            self._log(f"\n[例外] {e}\n")

# ----------------- GUI本体 -----------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1000x740")
        self.settings = load_settings()

        # 入力値
        self.var_date  = tk.StringVar(value=self.settings.get("date", today_jst_yyyymmdd()))
        self.var_jcd   = tk.StringVar(value=self.settings.get("jcd", "24"))
        self.var_race  = tk.StringVar(value=self.settings.get("race","12"))
        self.var_approach = tk.StringVar(value=self.settings.get("approach","base"))
        self.var_advanced = tk.BooleanVar(value=False)  # 起動時はOFF

        # モデルDIR（アプローチ別に保持）
        self.var_model_dir  = tk.StringVar(value=self.settings.get("model_dir_base", os.path.join("models","base","latest")))
        self.var_model_dir_map = {
            "base": self.settings.get("model_dir_base", os.path.join("models","base","latest")),
            "sectional": self.settings.get("model_dir_sectional", os.path.join("models","sectional","latest")),
        }
        self.var_online     = tk.BooleanVar(value=self.settings.get("use_online", False))  # build_live_rowに--online

        # CSV自動推定ON/OFF
        self.var_autoguess_csv = tk.BooleanVar(value=True)

        # 実行
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
        cmb.grid(row=0, column=7, padx=(5,0))
        cmb.bind("<<ComboboxSelected>>", self._on_change_approach)

        # ボタン群
        frm_btn = ttk.Frame(self); frm_btn.pack(fill=tk.X, padx=10, pady=6)
        self.btn_run_scrape = ttk.Button(frm_btn, text="▶ スクレイプ→推論", command=self.on_run_from_scrape, width=22)
        self.btn_run_scrape.pack(side=tk.LEFT)
        ttk.Label(frm_btn, text=" または ").pack(side=tk.LEFT, padx=6)
        self.btn_run_csv = ttk.Button(frm_btn, text="▶ CSVから推論", command=self.on_run_from_csv, width=18)
        self.btn_run_csv.pack(side=tk.LEFT, padx=(0,8))
        self.btn_stop = ttk.Button(frm_btn, text="■ 停止", command=self.on_stop, width=10, state=tk.DISABLED)
        self.btn_stop.pack(side=tk.LEFT, padx=6)

        # 右側：ログ操作・CSV自動推定
        ttk.Checkbutton(frm_btn, text="CSVファイル名を自動推定", variable=self.var_autoguess_csv).pack(side=tk.RIGHT, padx=(10,0))
        ttk.Button(frm_btn, text="🧹 ログをクリア", command=self._clear_log).pack(side=tk.RIGHT, padx=(6,0))
        ttk.Button(frm_btn, text="📁 出力フォルダ（data/live）", command=self._open_live_dir).pack(side=tk.RIGHT)

        # 詳細設定
        self.frm_adv = ttk.LabelFrame(self, text="詳細設定（モデル/実験オプション）")
        ttk.Label(self.frm_adv, text="モデルディレクトリ（選択アプローチに対応）").grid(row=0, column=0, sticky="w")
        ttk.Entry(self.frm_adv, textvariable=self.var_model_dir).grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(self.frm_adv, text="参照", command=self._browse_model_dir).grid(row=0, column=2, padx=6)
        self.frm_adv.columnconfigure(1, weight=1)

        ttk.Checkbutton(self.frm_adv, text="build_live_row に --online（検証/非常時のみ）",
                        variable=self.var_online).grid(row=1, column=0, columnspan=3, sticky="w", pady=(6,2))

        # 詳細設定の表示/非表示
        frm_toggle = ttk.Frame(self); frm_toggle.pack(fill=tk.X, padx=10, pady=(0,2))
        ttk.Checkbutton(frm_toggle, text="詳細設定（モデル/実験）", variable=self.var_advanced, command=self._toggle_adv).pack(side=tk.LEFT)
        if self.var_advanced.get():
            self.frm_adv.pack(fill=tk.X, padx=10, pady=(0,6))

        # ログ
        frm_log = ttk.LabelFrame(self, text="ログ/出力"); frm_log.pack(fill=tk.BOTH, expand=True, padx=10, pady=(6,10))
        self.txt_log = tk.Text(frm_log, wrap="word", height=24)
        self.txt_log.pack(fill=tk.BOTH, expand=True)

    # -------------- ユーティリティ --------------
    def _log(self, msg: str):
        t = datetime.now().strftime("%H:%M:%S")
        self.txt_log.insert(tk.END, f"[{t}] {msg}\n")
        self.txt_log.see(tk.END)

    def _clear_log(self):
        self.txt_log.delete("1.0", "end")

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
        if self.var_advanced.get():
            self.frm_adv.pack(fill=tk.X, padx=10, pady=(0,6))
        else:
            self.frm_adv.forget()

    def _browse_model_dir(self):
        d = filedialog.askdirectory(title=f"{self.var_approach.get()} モデルディレクトリ", initialdir=os.getcwd())
        if d:
            self.var_model_dir.set(d)
            self.var_model_dir_map[self.var_approach.get()] = d  # アプローチごとに保持

    def _on_change_approach(self, _evt=None):
        ap = self.var_approach.get()
        d = self.var_model_dir_map.get(ap, os.path.join("models", ap, "latest"))
        self.var_model_dir.set(d)

    def _open_live_dir(self):
        live_dir = os.path.join("data","live")
        Path(live_dir).mkdir(parents=True, exist_ok=True)
        if os.name == "nt":
            os.startfile(live_dir)
        elif sys.platform == "darwin":
            subprocess.Popen(["open", live_dir])
        else:
            subprocess.Popen(["xdg-open", live_dir])

    def _persist_common_settings(self):
        # アプローチごとにモデルDIRを保存
        self.var_model_dir_map[self.var_approach.get()] = self.var_model_dir.get().strip()
        save_settings({
            "date": self.var_date.get().strip(),
            "jcd": self.var_jcd.get().strip(),
            "race": self.var_race.get().strip(),
            "approach": self.var_approach.get().strip(),
            "model_dir_base": self.var_model_dir_map.get("base", os.path.join("models","base","latest")),
            "model_dir_sectional": self.var_model_dir_map.get("sectional", os.path.join("models","sectional","latest")),
            "use_online": bool(self.var_online.get()),
        })

    def _lock_ui(self, lock: bool):
        state = tk.DISABLED if lock else tk.NORMAL
        self.btn_run_scrape.config(state=state)
        self.btn_run_csv.config(state=state)
        self.btn_stop.config(state=tk.NORMAL if lock else tk.DISABLED)

    # -------------- 実行（フローA/B） --------------
    def on_run_from_scrape(self):
        date = self.var_date.get().strip()
        jcd  = self.var_jcd.get().strip()
        race = self.var_race.get().strip()
        approach = self.var_approach.get().strip()
        model_dir = self.var_model_dir.get().strip()

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

        self._persist_common_settings()

        # 区切り線（ログは消さない）
        self._log("="*72)
        self._log(f"開始(A): scrape→predict | date={date}, jcd={jcd}, race={race}, approach={approach}")
        self._log(f"model_dir={model_dir or f'models/{approach}/latest'}, online={'ON' if self.var_online.get() else 'OFF'}")
        self._log("="*72)

        self._lock_ui(True)
        self.runner.stop_flag.clear()
        self.worker_thread = threading.Thread(
            target=self.runner.run_from_scrape,
            args=(date, jcd, race, approach, model_dir, bool(self.var_online.get()), os.getcwd()),
            daemon=True
        )
        self.worker_thread.start()
        self.after(500, self._check_done)

    def on_run_from_csv(self):
        approach = self.var_approach.get().strip()
        model_dir = self.var_model_dir.get().strip()

        csv_path = None
        # 自動推定ONなら、フォーム値から推定
        if self.var_autoguess_csv.get():
            date = self.var_date.get().strip()
            jcd  = self.var_jcd.get().strip()
            race = self.var_race.get().strip()
            if valid_yyyymmdd(date) and jcd in JCD_CHOICES:
                try:
                    r = int(race); assert 1 <= r <= 12
                    guess = os.path.join("data","live", f"raw_{date}_{jcd}_{r}.csv")
                    if os.path.exists(guess):
                        csv_path = guess
                        self._log(f"[auto] 既存CSVを自動選択: {csv_path}")
                    else:
                        self._log(f"[auto] 想定パスにCSVなし: {guess} → ダイアログへ")
                except Exception:
                    self._log("[auto] レース番号の形式が不正 → ダイアログへ")

        # 見つからなければダイアログ
        if not csv_path:
            csv_path = filedialog.askopenfilename(
                title="推論用CSVを選択",
                initialdir=os.path.join(os.getcwd(), "data", "live"),
                filetypes=[("CSV files","*.csv"), ("All files","*.*")]
            )
            if not csv_path:
                return

        if approach not in APPROACH_CHOICES:
            messagebox.showerror("入力エラー","アプローチが不正です。"); return
        if not os.path.exists(SCRIPTS["predict_one_race"]):
            messagebox.showerror("ファイルなし", f"{SCRIPTS['predict_one_race']} が見つかりません。"); return

        self._persist_common_settings()

        # 区切り線（ログは消さない）
        self._log("="*72)
        self._log(f"開始(B): csv→predict | file={csv_path}, approach={approach}")
        self._log(f"model_dir={model_dir or f'models/{approach}/latest'}")
        self._log("="*72)

        self._lock_ui(True)
        self.runner.stop_flag.clear()
        self.worker_thread = threading.Thread(
            target=self.runner.run_from_csv,
            args=(csv_path, approach, model_dir, os.getcwd()),
            daemon=True
        )
        self.worker_thread.start()
        self.after(500, self._check_done)

    def _check_done(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self.after(500, self._check_done)
            return
        self._lock_ui(False)
        self._log("完了 / 停止")

    def on_stop(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self.runner.stop()
            self._log("停止要求を送信しました…")

if __name__ == "__main__":
    ensure_parent_dir(SETTINGS_FILE)
    app = App()
    app.mainloop()
