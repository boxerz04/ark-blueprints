# scripts/export_base_feature_yaml.py
#
# master.csv から「シンプルな列一覧 YAML」を生成するスクリプト。
# columns: [...] にフラットなリストとして書き出すだけ。
#
# 使い方:
#   python scripts/export_base_feature_yaml.py ^
#       --master data/processed/master.csv ^
#       --out    features/base.yaml

import argparse
import pandas as pd
import yaml
from datetime import datetime


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="入力 master.csv")
    ap.add_argument("--out", required=True, help="出力 YAML パス (features/base.yaml)")
    args = ap.parse_args()

    df = pd.read_csv(args.master, encoding="utf-8-sig")

    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_master": args.master,
        "n_rows": int(df.shape[0]),
        "n_cols": int(df.shape[1]),
        "columns": list(df.columns),  # 完全フラット
    }

    with open(args.out, "w", encoding="utf-8") as f:
        yaml.safe_dump(payload, f, allow_unicode=True, sort_keys=False)

    print(f"[OK] exported {args.out}")
    print(f"  rows={payload['n_rows']} cols={payload['n_cols']}")


if __name__ == "__main__":
    main()
