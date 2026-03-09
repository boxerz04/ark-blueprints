import argparse
import csv
import os
from collections import defaultdict


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Build venue-wise top3 lane share prior from payouts CSV.")
    parser.add_argument("--in-csv", default="data/processed/payouts/all_payout_results.csv", help="Input payouts CSV path")
    parser.add_argument("--out-csv", default="data/priors/payouts_regime/latest.csv", help="Output prior CSV path")
    parser.add_argument("--start-date", default=None, help="Start date filter (YYYYMMDD, inclusive)")
    parser.add_argument("--end-date", default=None, help="End date filter (YYYYMMDD, inclusive)")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    in_csv = resolve_path(project_root, args.in_csv)
    out_csv = resolve_path(project_root, args.out_csv)

    if not os.path.exists(in_csv):
        raise FileNotFoundError(f"input CSV not found: {in_csv}")

    venue_lane_counts: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    venue_races: dict[str, set[tuple[str, str]]] = defaultdict(set)
    matched_date_rows = 0

    with open(in_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"日付", "場名", "レース番号", "1着", "2着", "3着"}
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
            if not date or not venue or not race_no:
                continue

            lanes = [parse_lane(row.get("1着")), parse_lane(row.get("2着")), parse_lane(row.get("3着"))]
            if any(l is None for l in lanes):
                continue

            venue_races[venue].add((date, race_no))
            for lane in lanes:
                venue_lane_counts[venue][lane] += 1

    if matched_date_rows == 0:
        raise ValueError("no rows matched date filter")

    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    rows = []
    for venue in sorted(venue_lane_counts.keys()):
        total_count = sum(venue_lane_counts[venue][lane] for lane in range(1, 7))
        race_count = len(venue_races[venue])
        expected_total = race_count * 3
        if total_count != expected_total:
            raise ValueError(
                f"count mismatch at venue={venue}: total_count={total_count}, expected={expected_total}"
            )

        for lane in range(1, 7):
            count = venue_lane_counts[venue][lane]
            share = count / total_count if total_count > 0 else 0.0
            rows.append({"場名": venue, "lane": lane, "count": count, "share": share})

        share_sum = sum(r["share"] for r in rows if r["場名"] == venue)
        if abs(share_sum - 1.0) > 1e-9:
            raise ValueError(f"share sum mismatch at venue={venue}: {share_sum}")

    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["場名", "lane", "count", "share"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] wrote prior CSV: {out_csv} (rows={len(rows)})")


if __name__ == "__main__":
    main()
