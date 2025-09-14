# gui_predict_one_race.py
# 1レース推論 GUI（詳細設定・買い目候補・堅牢パース版）
# - settings.json の保存先を data/config/settings.json に変更
# - 旧 ./settings.json があれば読み込んで自動移行

import os, sys, re, csv, json, queue, signal, threading, subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

APP_TITLE = "Boatrace 1レース推論 GUI"

# 新しい保存先
SETTINGS_FILE = os.path.join("data", "config", "settings.json")
# 旧バージョン互換（存在すれば読み込んで移行）
LEGACY_SETTINGS_FILE = "settings.json"

SCRIPTS = {
    "scrape_one_race": os.path.join("scripts", "scrape_one_race.py"),
    "build_live_row": os.path.join("scripts", "build_live_row.py"),
    "predict_one_race": os.path.join("scripts", "predict_one_race.py"),
    "predict_top2pair": os.path.join("scripts", "predict_top2pair.py"),
}

JCD_CHOICES = [f"{i:02d}" for i in range(1, 25)]
RACE_CHOICES = [f"{i}" for i in range(1, 13)]

def ensure_parent_dir(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

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

def save_settings(state: dict):
    try:
        ensure_parent_dir(SETTINGS_FILE)
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def load_settings() -> dict:
    # 新パスがあればそれを使う
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    # 新パスが無く、旧 ./settings.json があれば読み込んで移行
    if os.path.exists(LEGACY_SETTINGS_FILE):
        try:
            with open(LEGACY_SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            save_settings(data)  # 新パスへ保存
            return data
        except Exception:
            return {}
    return {}

class Runner:
    """サブプロセス実行＋ログ転送。baseの確率表をログからパースして保持。"""
    def __init__(self, log_queue: queue.Queue):
        self.log_queue = log_queue
        self.stop_flag = threading.Event()
        self.current_proc = None
        self.last_out_csv = None
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

    def _run_and_stream(self, cmd, cwd=None, capture_base_probs=False):
        if self.stop_flag.is_set():
            return 1
        self._log(f"\n$ {' '.join(map(str, cmd))}\n")

        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        preexec_fn = None
        if os.name != "nt":
            import os as _os
            preexec_fn = _os.setsid

        self.current_proc = subprocess.Popen(
            cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, universal_newlines=True,
            creationflags=creationflags, preexec_fn=preexec_fn,
        )

        in_summary = False
        skip_header_next = False

        for raw in self.current_proc.stdout:
            line = raw.rstrip("\n")
            self._log(line)

            if capture_base_probs:
                # [SUMMARY] prob(desc): ブロック開始検知
                if line.strip().startswith("[SUMMARY]") and "prob" in line:
                    in_summary = True
                    skip_header_next = True  # 次行はヘッダ
                    continue
                if in_summary:
                    if not line.strip():
                        in_summary = False
                        continue
                    if skip_header_next:
                        skip_header_next = False
                        continue
                    # 期待: race_id code R wakuban player proba
                    tokens = line.split()
                    if len(tokens) >= 6:
                        try:
                            wakuban = int(tokens[3])
                            proba = float(tokens[-1])
                            if 1 <= wakuban <= 6:
                                self.base_probs[wakuban] = proba
                        except Exception:
                            pass

        rc = self.current_proc.wait()
        self.current_proc = None
        self._log(f"[exit code] {rc}\n")
        return rc

    def run_pipeline(
        self, date_yyyymmdd: str, jcd: str, race: str,
        model_dir_base: str, use_online: bool, also_top2pair: bool,
        repo_root: str, adv_enabled: bool, top2_model_path: str | None,
    ):
        self.last_out_csv = None
        self.last_top2_csv = None
        self.base_probs = {}
        self.last_race_id = f"{date_yyyymmdd}{jcd}{int(race):02d}"

        try:
            for key in ("scrape_one_race", "build_live_row", "predict_one_race"):
                if not os.path.exists(SCRIPTS[key]):
                    self._log(f"ERROR: {SCRIPTS[key]} が見つかりません。リポジトリ直下で実行してください。")
                    return

            if not model_dir_base:
                model_dir_base = os.path.join("models", "base", "latest")
            model_pkl = os.path.join(model_dir_base, "model.pkl")
            feat_pkl  = os.path.join(model_dir_base, "feature_pipeline.pkl")
            if not os.path.exists(model_pkl) or not os.path.exists(feat_pkl):
                self._log(f"ERROR: base モデル一式が見つかりません。\n  model: {model_pkl}\n  feature_pipeline: {feat_pkl}")
                return

            # 1) scrape
            rc = self._run_and_stream(
                [sys.executable, SCRIPTS["scrape_one_race"], "--date", date_yyyymmdd, "--jcd", jcd, "--race", race],
                cwd=repo_root
            )
            if rc != 0 or self.stop_flag.is_set(): return

            # 2) live row
            out_csv = os.path.join("data", "live", f"raw_{date_yyyymmdd}_{jcd}_{race}.csv")
            cmd2 = [sys.executable, SCRIPTS["build_live_row"],
                    "--date", date_yyyymmdd, "--jcd", jcd, "--race", race, "--out", out_csv]
            if use_online: cmd2.append("--online")
            rc = self._run_and_stream(cmd2, cwd=repo_root)
            if rc != 0 or self.stop_flag.is_set(): return
            self.last_out_csv = out_csv

            # 3) base predict（確率パースON）
            rc = self._run_and_stream(
                [sys.executable, SCRIPTS["predict_one_race"], "--live-csv", out_csv,
                 "--model", model_pkl, "--feature-pipeline", feat_pkl],
                cwd=repo_root, capture_base_probs=True
            )
            if rc != 0 or self.stop_flag.is_set(): return

            # 4) top2pair
            if also_top2pair and os.path.exists(SCRIPTS["predict_top2pair"]):
                race_id = f"{date_yyyymmdd}{jcd}{int(race):02d}"
                cmd4 = [sys.executable, SCRIPTS["predict_top2pair"],
                        "--mode", "live", "--master", out_csv, "--race-id", race_id]
                if adv_enabled and top2_model_path and os.path.exists(top2_model_path):
                    cmd4 += ["--model", top2_model_path]
                rc = self._run_and_stream(cmd4, cwd=repo_root)
                if rc != 0 or self.stop_flag.is_set(): return
                tcsv = os.path.join("data", "live", "top2pair", f"pred_{date_yyyymmdd}_{jcd}_{race}.csv")
                if os.path.exists(tcsv):
                    self.last_top2_csv = tcsv

            self._log("\n=== すべて完了しました ✅ ===\n")

        except Exception as e:
            self._log(f"\n[例外] {e}\n")

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("980x720")
        self.settings = load_settings()

        # 入力
        frm_top = ttk.Frame(self); frm_top.pack(fill=tk.X, padx=10, pady=(10,5))
        ttk.Label(frm_top, text="日付 (YYYYMMDD)").grid(row=0, column=0, sticky="w")
        self.var_date = tk.StringVar(value=self.settings.get("date", today_jst_yyyymmdd()))
        ttk.Entry(frm_top, textvariable=self.var_date, width=12).grid(row=0, column=1, padx=(5,15))
        ttk.Label(frm_top, text="場コード").grid(row=0, column=2, sticky="w")
        self.var_jcd = tk.StringVar(value=self.settings.get("jcd","12"))
        ttk.Combobox(frm_top, textvariable=self.var_jcd, values=JCD_CHOICES, width=5, state="readonly").grid(row=0, column=3, padx=(5,15))
        ttk.Label(frm_top, text="レース").grid(row=0, column=4, sticky="w")
        self.var_race = tk.StringVar(value=self.settings.get("race","12"))
        ttk.Combobox(frm_top, textvariable=self.var_race, values=RACE_CHOICES, width=5, state="readonly").grid(row=0, column=5, padx=(5,15))

        # オプション
        frm_opt = ttk.Frame(self); frm_opt.pack(fill=tk.X, padx=10, pady=5)
        self.var_online = tk.BooleanVar(value=self.settings.get("use_online", False))
        self.var_top2 = tk.BooleanVar(value=self.settings.get("also_top2pair", True))
        self.var_advanced = tk.BooleanVar(value=self.settings.get("advanced", False))
        ttk.Checkbutton(frm_opt, text="build_live_row に --online（通常不要）", variable=self.var_online).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(frm_opt, text="top2pair も同時に推論", variable=self.var_top2).grid(row=0, column=1, sticky="w", padx=(20,0))
        ttk.Checkbutton(frm_opt, text="詳細設定（モデル手動選択）", variable=self.var_advanced, command=self.toggle_advanced)\
            .grid(row=0, column=2, sticky="w", padx=(20,0))

        # 詳細設定
        self.frm_adv = ttk.Frame(self); self.frm_adv.pack(fill=tk.X, padx=10, pady=(0,5))
        ttk.Label(self.frm_adv, text="base: モデルディレクトリ").grid(row=0, column=0, sticky="w")
        self.var_model_dir = tk.StringVar(value=self.settings.get("model_dir", os.path.join("models","base","latest")))
        ent_model = ttk.Entry(self.frm_adv, textvariable=self.var_model_dir); ent_model.grid(row=0, column=1, sticky="we", padx=5)
        ttk.Button(self.frm_adv, text="参照", command=self.browse_model_base).grid(row=0, column=2, padx=5)
        self.frm_adv.columnconfigure(1, weight=1)
        ttk.Label(self.frm_adv, text="top2pair: model.pkl").grid(row=1, column=0, sticky="w")
        self.var_top2_model = tk.StringVar(value=self.settings.get("top2_model", os.path.join("models","top2pair","latest","model.pkl")))
        ent_top2 = ttk.Entry(self.frm_adv, textvariable=self.var_top2_model); ent_top2.grid(row=1, column=1, sticky="we", padx=5)
        ttk.Button(self.frm_adv, text="参照", command=self.browse_model_top2).grid(row=1, column=2, padx=5)
        if not self.var_advanced.get(): self.frm_adv.forget()

        # ボタン
        frm_btn = ttk.Frame(self); frm_btn.pack(fill=tk.X, padx=10, pady=5)
        self.btn_run = ttk.Button(frm_btn, text="▶ 推論開始", command=self.on_run, width=22); self.btn_run.pack(side=tk.LEFT)
        self.btn_stop = ttk.Button(frm_btn, text="■ 停止", command=self.on_stop, width=12, state=tk.DISABLED); self.btn_stop.pack(side=tk.LEFT, padx=8)
        self.btn_open = ttk.Button(frm_btn, text="📂 出力フォルダ（data/live）", command=self.open_live_dir); self.btn_open.pack(side=tk.RIGHT)
        self.btn_kai = ttk.Button(frm_btn, text="🎯 買い目候補の生成", command=self.on_generate_tickets, state=tk.DISABLED); self.btn_kai.pack(side=tk.RIGHT, padx=8)

        # ログ
        frm_log = ttk.Frame(self); frm_log.pack(fill=tk.BOTH, expand=True, padx=10, pady=(5,10))
        self.txt = tk.Text(frm_log, wrap="word"); self.txt.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        yscroll = ttk.Scrollbar(frm_log, orient=tk.VERTICAL, command=self.txt.yview); yscroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.txt.configure(yscrollcommand=yscroll.set)

        # ステータス
        self.var_status = tk.StringVar(value="待機中")
        status = ttk.Label(self, textvariable=self.var_status, anchor="w", relief=tk.SUNKEN); status.pack(fill=tk.X, side=tk.BOTTOM)

        # 実行系
        self.log_queue = queue.Queue()
        self.runner = Runner(self.log_queue)
        self.worker_thread = None
        self.after(50, self.poll_log_queue)

    # ==== UI helpers ====
    def append_log(self, text: str):
        self.txt.insert(tk.END, text + "\n"); self.txt.see(tk.END)

    def poll_log_queue(self):
        try:
            while True:
                self.append_log(self.log_queue.get_nowait())
        except queue.Empty:
            pass
        finally:
            self.after(50, self.poll_log_queue)

    def open_live_dir(self):
        live_dir = os.path.join("data","live"); Path(live_dir).mkdir(parents=True, exist_ok=True)
        if os.name == "nt": os.startfile(live_dir)
        elif sys.platform == "darwin": subprocess.Popen(["open", live_dir])
        else: subprocess.Popen(["xdg-open", live_dir])

    def toggle_advanced(self):
        if self.var_advanced.get(): self.frm_adv.pack(fill=tk.X, padx=10, pady=(0,5))
        else: self.frm_adv.forget()

    def browse_model_base(self):
        d = filedialog.askdirectory(title="base モデルディレクトリ", initialdir=os.getcwd())
        if d: self.var_model_dir.set(d)

    def browse_model_top2(self):
        f = filedialog.askopenfilename(title="top2pair model.pkl を選択", initialdir=os.getcwd(),
                                       filetypes=[("Pickle","*.pkl"),("All","*.*")])
        if f: self.var_top2_model.set(f)

    # ==== Run/Stop ====
    def on_run(self):
        date = self.var_date.get().strip()
        jcd = self.var_jcd.get().strip()
        race = self.var_race.get().strip()
        model_dir = self.var_model_dir.get().strip()
        use_online = self.var_online.get()
        also_top2pair = self.var_top2.get()
        advanced = self.var_advanced.get()
        top2_model = self.var_top2_model.get().strip() if advanced else None

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

        save_settings({
            "date": date, "jcd": jcd, "race": race, "model_dir": model_dir,
            "use_online": use_online, "also_top2pair": also_top2pair,
            "advanced": advanced, "top2_model": self.var_top2_model.get().strip(),
        })

        self.btn_run.config(state=tk.DISABLED); self.btn_stop.config(state=tk.NORMAL)
        self.btn_kai.config(state=tk.DISABLED); self.var_status.set("実行中…")
        self.append_log("="*70)
        self.append_log(f"開始: date={date}, jcd={jcd}, race={race}")
        self.append_log(f"base={model_dir or 'models/base/latest'}, top2pair={'custom' if (advanced and top2_model) else 'latest'}")
        self.append_log("="*70)

        self.runner.stop_flag.clear()
        self.worker_thread = threading.Thread(
            target=self.runner.run_pipeline,
            args=(date, jcd, race, model_dir, use_online, also_top2pair, os.getcwd(), advanced, top2_model),
            daemon=True
        )
        self.worker_thread.start()
        self.after(500, self.check_thread_done)

    def check_thread_done(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self.after(500, self.check_thread_done); return
        self.btn_run.config(state=tk.NORMAL); self.btn_stop.config(state=tk.DISABLED)
        if self.runner.last_top2_csv or self.runner.base_probs:
            self.btn_kai.config(state=tk.NORMAL)
        self.var_status.set("完了 / 停止")

    def on_stop(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self.runner.stop(); self.var_status.set("停止要求を送信しました…")

    # ==== 買い目候補 ====
    def on_generate_tickets(self):
        top2_csv = self.runner.last_top2_csv
        base_probs = self.runner.base_probs.copy()
        race_id = self.runner.last_race_id
        if not (top2_csv and os.path.exists(top2_csv)):
            messagebox.showwarning("データ不足","top2pair の結果CSVが見つかりません。先に推論を実行してください。"); return
        if not base_probs:
            messagebox.showwarning("データ不足","base の確率表が取得できませんでした。先に推論を実行してください。"); return

        # まずCSVから読み取り（区切り推定＋列名ゆらぎ対応）
        pairs = self._read_top2_pairs(top2_csv)
        # CSVが空ならログからフォールバック
        if not pairs:
            pairs = self._read_top2_pairs_from_log(self.txt.get("1.0", tk.END))
        if not pairs:
            messagebox.showwarning("データ不足","top2pair のペアが取得できませんでした（CSV/ログともに読めず）。"); 
            return

        # 2連単スコア
        tickets_2rentan = []
        for (i, j, p) in pairs:
            bi, bj = base_probs.get(i, 1e-6), base_probs.get(j, 1e-6)
            if bi >= bj:
                head, tail, headp, tailp = i, j, bi, bj
            else:
                head, tail, headp, tailp = j, i, bj, bi
            score = p * (0.6*headp + 0.4*tailp)
            tickets_2rentan.append((f"{head}-{tail}", score))
        tickets_2rentan.sort(key=lambda x: x[1], reverse=True)
        top2rentan = tickets_2rentan[:10]

        # 3連複スコア
        tickets_3renpuku = []
        for (i, j, p) in pairs[:30]:
            bi, bj = base_probs.get(i, 1e-6), base_probs.get(j, 1e-6)
            third, thirdp = None, -1.0
            for k in range(1, 7):
                if k in (i, j): continue
                bk = base_probs.get(k, 1e-6)
                if bk > thirdp: thirdp, third = bk, k
            if third is None: continue
            trio = tuple(sorted([i, j, third]))
            fmt = f"{trio[0]}-{trio[1]}-{trio[2]}"
            score = p * (0.5*max(bi, bj) + 0.5*thirdp)
            tickets_3renpuku.append((fmt, score))
        # 重複は最大スコア採用でユニーク化
        agg = {}
        for fmt, sc in tickets_3renpuku:
            agg[fmt] = max(agg.get(fmt, 0.0), sc)
        tickets_3renpuku = sorted(agg.items(), key=lambda x: x[1], reverse=True)[:10]

        # 出力
        self.append_log("\n=== 🎯買い目候補（暫定スコア） ===")
        if race_id: self.append_log(f"race_id: {race_id}")
        self.append_log("\n[2連単 TOP10]")
        for fmt, sc in top2rentan: self.append_log(f"{fmt}\t score={sc:.6f}")
        self.append_log("\n[3連複 TOP10]")
        for fmt, sc in tickets_3renpuku: self.append_log(f"{fmt}\t score={sc:.6f}")
        self.append_log("\n※ スコアは base(頭/相手の確率) と top2pair の p_top2set を掛け合わせた暫定指標。直前オッズと組み合わせてEV/ケリー配分に落とすのがおすすめ。")

    # ---- helpers: top2pair CSV/ログ パース ----
    def _read_top2_pairs(self, csv_path: str):
        """CSV から (i, j, p_top2set) を堅牢に取得（区切り自動推定・列名ゆらぎ対応）。"""
        try:
            with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
                sample = f.read(2048); f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",\t; ")
                except Exception:
                    class _D: delimiter = ","
                    dialect = _D()
                reader = csv.DictReader(f, dialect=dialect, skipinitialspace=True)
                if not reader.fieldnames:
                    return []
                norm = { (c or "").strip().lower(): c for c in reader.fieldnames }
                def pick(row, keys):
                    for k in keys:
                        c = norm.get(k)
                        if c is not None and row.get(c, "") != "":
                            return row[c]
                    for c in row.keys():
                        if (c or "").strip().lower() in keys and row.get(c, "") != "":
                            return row[c]
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
            try:
                pairs = []
                with open(csv_path, "r", encoding="utf-8-sig") as f:
                    header = f.readline()
                    for line in f:
                        line = line.strip()
                        if not line: continue
                        cols = [c.strip() for c in re.split(r"[,\t]+", line)]
                        if len(cols) >= 4:
                            i, j, p = int(cols[1]), int(cols[2]), float(cols[3])
                            if 1 <= i <= 6 and 1 <= j <= 6 and i != j:
                                pairs.append((i, j, p))
                return pairs
            except Exception:
                return []

    def _read_top2_pairs_from_log(self, log_text: str):
        """ログの [TOP10 pairs by p_top2set] 表から (i, j, p_top2set) を抽出。"""
        pairs = []
        in_block = False
        for line in log_text.splitlines():
            if "[TOP10 pairs by p_top2set]" in line:
                in_block = True
                continue
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
    # 新設定ファイルの親ディレクトリは必ず作成
    ensure_parent_dir(SETTINGS_FILE)
    app = App()
    app.mainloop()
