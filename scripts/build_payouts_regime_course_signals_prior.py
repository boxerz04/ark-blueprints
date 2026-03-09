import argparse
import csv
import math
import os
from collections import defaultdict
from statistics import median


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

def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and parent != ".":
        os.makedirs(parent, exist_ok=True)



def parse_numeric(value) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Build venue-wise course-signal prior from payouts CSV.")
    parser.add_argument("--in-csv", default="data/processed/payouts/all_payout_results.csv", help="Input payouts CSV path")
    parser.add_argument(
        "--out-csv",
        default="data/priors/payouts_regime/course_signals_latest.csv",
        help="Output prior CSV path",
    )
    parser.add_argument("--start-date", default=None, help="Start date filter (YYYYMMDD, inclusive)")
    parser.add_argument("--end-date", default=None, help="End date filter (YYYYMMDD, inclusive)")
    parser.add_argument("--alpha", type=float, default=0.5, help="Additive smoothing strength (>=0)")
    args = parser.parse_args()

    if args.alpha < 0:
        raise ValueError(f"alpha must be >= 0: {args.alpha}")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    in_csv = resolve_path(project_root, args.in_csv)
    out_csv = resolve_path(project_root, args.out_csv)

    if not os.path.exists(in_csv):
        raise FileNotFoundError(f"input CSV not found: {in_csv}")

    venue_win_counts: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    venue_top2_counts: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    venue_top3_counts: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    venue_races: dict[str, set[tuple[str, str]]] = defaultdict(set)
    venue_popularity_values: dict[str, list[float]] = defaultdict(list)
    venue_log_payout_values: dict[str, list[float]] = defaultdict(list)
    matched_date_rows = 0

    with open(in_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"日付", "場名", "レース番号", "1着", "2着", "3着", "人気", "払戻金"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise KeyError(f"missing required columns: {sorted(missing)}")

        for row in reader:
            date = str(row.get("日付", "")).strip()
            if not date:
                continue
            if args.start_date and date < args.start_date:
                continue
            if args.end_date and date > args.end_date:
                continue

            matched_date_rows += 1

            venue = str(row.get("場名", "")).strip()
            race_no = str(row.get("レース番号", "")).strip()
            if not venue or not race_no:
                continue

            popularity_value = parse_numeric(row.get("人気"))
            if popularity_value is not None:
                venue_popularity_values[venue].append(popularity_value)

            payout_value = parse_numeric(row.get("払戻金"))
            if payout_value is not None:
                venue_log_payout_values[venue].append(math.log1p(payout_value))

            win_lane = parse_lane(row.get("1着"))
            top2_lanes = [parse_lane(row.get("1着")), parse_lane(row.get("2着"))]
            top3_lanes = [parse_lane(row.get("1着")), parse_lane(row.get("2着")), parse_lane(row.get("3着"))]
            if win_lane is None or any(l is None for l in top2_lanes) or any(l is None for l in top3_lanes):
                continue

            venue_races[venue].add((date, race_no))
            venue_win_counts[venue][win_lane] += 1
            for lane in top2_lanes:
                venue_top2_counts[venue][lane] += 1
            for lane in top3_lanes:
                venue_top3_counts[venue][lane] += 1

    if matched_date_rows == 0:
        raise ValueError("no rows matched date filter")

    rows = []
    for venue in sorted(venue_races.keys()):
        n_races = len(venue_races[venue])
        if n_races == 0:
            continue

        popularity_values = venue_popularity_values[venue]
        log_payout_values = venue_log_payout_values[venue]
        n_popularity_valid = len(popularity_values)
        n_payout_valid = len(log_payout_values)
        if n_popularity_valid == 0:
            raise ValueError(f"no valid numeric popularity rows for venue={venue}; cannot build popularity prior")
        if n_payout_valid == 0:
            raise ValueError(f"no valid numeric payout rows for venue={venue}; cannot build payout prior")

        win_total = sum(venue_win_counts[venue][lane] for lane in range(1, 7))
        top2_total = sum(venue_top2_counts[venue][lane] for lane in range(1, 7))
        top3_total = sum(venue_top3_counts[venue][lane] for lane in range(1, 7))

        if win_total != n_races:
            raise ValueError(
                f"win count mismatch at venue={venue}: win_total={win_total}, expected={n_races}"
            )
        expected_top2_total = 2 * n_races
        if top2_total != expected_top2_total:
            raise ValueError(
                f"top2 count mismatch at venue={venue}: top2_total={top2_total}, expected={expected_top2_total}"
            )
        expected_top3_total = 3 * n_races
        if top3_total != expected_top3_total:
            raise ValueError(
                f"top3 count mismatch at venue={venue}: top3_total={top3_total}, expected={expected_top3_total}"
            )

        row = {
            "場名": venue,
            "n_races": n_races,
            "n_popularity_valid": n_popularity_valid,
            "base_popularity_median": median(popularity_values),
            "n_payout_valid": n_payout_valid,
            "base_log_payout_median": median(log_payout_values),
        }
        win_denom = n_races + 6 * args.alpha
        top2_denom = expected_top2_total + 6 * args.alpha
        top3_denom = expected_top3_total + 6 * args.alpha
        for lane in range(1, 7):
            row[f"base_win_{lane}"] = (venue_win_counts[venue][lane] + args.alpha) / win_denom
            row[f"base_top2_{lane}"] = (venue_top2_counts[venue][lane] + args.alpha) / top2_denom
            row[f"base_top3_{lane}"] = (venue_top3_counts[venue][lane] + args.alpha) / top3_denom

        win_sum = sum(row[f"base_win_{lane}"] for lane in range(1, 7))
        top2_sum = sum(row[f"base_top2_{lane}"] for lane in range(1, 7))
        top3_sum = sum(row[f"base_top3_{lane}"] for lane in range(1, 7))
        if not math.isclose(win_sum, 1.0, abs_tol=1e-9):
            raise ValueError(f"base_win sum mismatch at venue={venue}: {win_sum}")
        if not math.isclose(top2_sum, 1.0, abs_tol=1e-9):
            raise ValueError(f"base_top2 sum mismatch at venue={venue}: {top2_sum}")
        if not math.isclose(top3_sum, 1.0, abs_tol=1e-9):
            raise ValueError(f"base_top3 sum mismatch at venue={venue}: {top3_sum}")

        rows.append(row)

    if not rows:
        raise ValueError("no valid venue rows produced from input data")

    ensure_parent_dir(out_csv)

    fieldnames = [
        "場名",
        "n_races",
        "n_popularity_valid",
        "base_popularity_median",
        "n_payout_valid",
        "base_log_payout_median",
        *(f"base_win_{i}" for i in range(1, 7)),
        *(f"base_top2_{i}" for i in range(1, 7)),
        *(f"base_top3_{i}" for i in range(1, 7)),
    ]
    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] wrote prior CSV: {out_csv} (venues={len(rows)})")


if __name__ == "__main__":
    main()
