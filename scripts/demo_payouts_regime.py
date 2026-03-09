import argparse
import csv
import os
from collections import defaultdict


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

def resolve_path(project_root: str, path: str) -> str:
    return path if os.path.isabs(path) else os.path.join(project_root, path)


def parse_lane(value) -> int | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith('.0'):
        s = s[:-2]
    if not s.isdigit():
        return None
    lane = int(s)
    return lane if 1 <= lane <= 6 else None


def parse_race_no(race_no: str) -> int:
    s = str(race_no).strip().upper().replace("R", "")
    return int(s)


def load_prior(prior_csv: str, venue: str) -> dict[int, float]:
    base_share: dict[int, float] = {}
    with open(prior_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"場名", "lane", "share"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise KeyError(f"prior CSV missing required columns: {sorted(missing)}")

        for row in reader:
            if str(row.get("場名", "")).strip() != venue:
                continue
            lane = parse_lane(row.get("lane"))
            if lane is None:
                continue
            share = float(row.get("share", 0.0))
            base_share[lane] = share

    for lane in range(1, 7):
        if lane not in base_share:
            raise ValueError(f"prior is missing lane={lane} for venue={venue}")
    if abs(sum(base_share.values()) - 1.0) > 1e-9:
        raise ValueError(f"prior share sum != 1.0 at venue={venue}")
    return base_share


def load_day_races(payout_csv: str, date: str, venue: str) -> list[dict]:
    races = []
    with open(payout_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"日付", "場名", "レース番号", "1着", "2着", "3着"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise KeyError(f"payout CSV missing required columns: {sorted(missing)}")

        for row in reader:
            if str(row.get("日付", "")).strip() != date:
                continue
            if str(row.get("場名", "")).strip() != venue:
                continue

            try:
                race_no_int = parse_race_no(row.get("レース番号", ""))
            except Exception:
                continue

            lanes = [parse_lane(row.get("1着")), parse_lane(row.get("2着")), parse_lane(row.get("3着"))]
            if any(l is None for l in lanes):
                continue

            races.append(
                {
                    "日付": date,
                    "場名": venue,
                    "レース番号": f"{race_no_int}R",
                    "race_no_int": race_no_int,
                    "top3_lanes": lanes,
                }
            )

    races.sort(key=lambda r: r["race_no_int"])
    return races


def build_sequential_rows(date: str, venue: str, races: list[dict], base_share: dict[int, float], prior_strength: float) -> list[dict]:
    observed_counts = defaultdict(int)
    rows = []

    for i, race in enumerate(races):
        observed_races = i
        total_observed = 3 * observed_races

        p_post = {}
        m_top3 = {}
        for lane in range(1, 7):
            p = (prior_strength * base_share[lane] + observed_counts[lane]) / (prior_strength + total_observed)
            p_post[lane] = p
            m_top3[lane] = p / base_share[lane]

        strength = sum(abs(p_post[lane] - base_share[lane]) for lane in range(1, 7))

        row = {
            "日付": date,
            "場名": venue,
            "レース番号": race["レース番号"],
            "observed_races": observed_races,
            "strength": strength,
        }
        for lane in range(1, 7):
            row[f"m_top3_{lane}"] = m_top3[lane]
            row[f"base_share_{lane}"] = base_share[lane]
            row[f"p_post_{lane}"] = p_post[lane]
        rows.append(row)

        for lane in race["top3_lanes"]:
            observed_counts[lane] += 1

    return rows


def print_rows(rows: list[dict]) -> None:
    if not rows:
        print("[WARN] no rows to display")
        return

    header = ["日付", "場名", "レース番号", "observed_races"] + [f"m_top3_{i}" for i in range(1, 7)] + ["strength"]
    print(",".join(header))
    for row in rows:
        values = [
            row["日付"],
            row["場名"],
            row["レース番号"],
            str(row["observed_races"]),
        ]
        values += [f"{row[f'm_top3_{i}']:.6f}" for i in range(1, 7)]
        values += [f"{row['strength']:.6f}"]
        print(",".join(values))


def write_rows_csv(out_csv: str, rows: list[dict]) -> None:
    if not rows:
        return
    ensure_parent_dir(out_csv)
    fieldnames = [
        "日付", "場名", "レース番号", "observed_races",
        *(f"m_top3_{i}" for i in range(1, 7)),
        "strength",
        *(f"base_share_{i}" for i in range(1, 7)),
        *(f"p_post_{i}" for i in range(1, 7)),
    ]
    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Demo sequential regime update from payouts top3 lanes.")
    parser.add_argument("--date", required=True, help="target date (YYYYMMDD)")
    parser.add_argument("--venue", required=True, help="target venue name")
    parser.add_argument("--prior-strength", type=float, default=60.0, help="prior strength K")
    parser.add_argument("--payout-csv", default="data/processed/payouts/all_payout_results.csv", help="payout CSV path")
    parser.add_argument("--prior-csv", default="data/priors/payouts_regime/latest.csv", help="prior CSV path")
    parser.add_argument("--out-csv", default=None, help="optional output CSV path")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)

    payout_csv = resolve_path(project_root, args.payout_csv)
    prior_csv = resolve_path(project_root, args.prior_csv)
    out_csv = resolve_path(project_root, args.out_csv) if args.out_csv else None

    if not os.path.exists(payout_csv):
        raise FileNotFoundError(f"payout CSV not found: {payout_csv}")
    if not os.path.exists(prior_csv):
        raise FileNotFoundError(f"prior CSV not found: {prior_csv}")

    base_share = load_prior(prior_csv, args.venue)
    races = load_day_races(payout_csv, args.date, args.venue)
    if not races:
        print(f"[WARN] no races found for date={args.date}, venue={args.venue}")
        return

    rows = build_sequential_rows(args.date, args.venue, races, base_share, args.prior_strength)
    print_rows(rows)

    if out_csv:
        write_rows_csv(out_csv, rows)
        print(f"[OK] wrote demo CSV: {out_csv}")


if __name__ == "__main__":
    main()
