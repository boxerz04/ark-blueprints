# src/model_utils.py
from pathlib import Path
from datetime import datetime
import json
import joblib
import shutil

def gen_model_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def prepare_dirs(approach: str, model_id: str):
    base = Path("models") / approach
    latest = base / "latest"
    run = base / "runs" / model_id
    latest.mkdir(parents=True, exist_ok=True)
    run.mkdir(parents=True, exist_ok=True)
    return latest, run

def save_artifacts(approach: str, model_id: str, artifacts: dict):
    """artifacts: {filename: object or filepath}"""
    latest, run = prepare_dirs(approach, model_id)
    for name, obj in artifacts.items():
        if isinstance(obj, (dict, list)):  # JSON
            for target in (latest / name, run / name):
                with open(target, "w", encoding="utf-8") as f:
                    json.dump(obj, f, ensure_ascii=False, indent=2)
        elif isinstance(obj, str) and Path(obj).exists():  # ファイルパス
            for target in (latest / name, run / name):
                shutil.copy2(obj, target)
        else:  # モデルなどpickle対象
            for target in (latest / name, run / name):
                joblib.dump(obj, target)
 
