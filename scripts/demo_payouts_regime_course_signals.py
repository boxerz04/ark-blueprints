import argparse
import csv
import os


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


def load_prior(prior_csv: str, venue: str) -> tuple[dict[int, float], dict[int, float], dict[int, float]]:
    base_win: dict[int, float] = {}
    base_top2: dict[int, float] = {}
    base_top3: dict[int, float] = {}
    with open(prior_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {
            "場名",
            *(f"base_win_{i}" for i in range(1, 7)),
            *(f"base_top2_{i}" for i in range(1, 7)),
            *(f"base_top3_{i}" for i in range(1, 7)),
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise KeyError(f"prior CSV missing required columns: {sorted(missing)}")

        for row in reader:
            if str(row.get("場名", "")).strip() != venue:
                continue
            for lane in range(1, 7):
                base_win[lane] = float(row[f"base_win_{lane}"])
                base_top2[lane] = float(row[f"base_top2_{lane}"])
                base_top3[lane] = float(row[f"base_top3_{lane}"])
            break

    for lane in range(1, 7):
        if lane not in base_win or lane not in base_top2 or lane not in base_top3:
            raise ValueError(f"prior is missing lane={lane} for venue={venue}")
        if base_win[lane] <= 0.0 or base_top2[lane] <= 0.0 or base_top3[lane] <= 0.0:
            raise ValueError(f"prior must be > 0.0 for multiplicative signal at venue={venue}, lane={lane}")

    if abs(sum(base_win.values()) - 1.0) > 1e-9:
        raise ValueError(f"base_win sum != 1.0 at venue={venue}")
    if abs(sum(base_top2.values()) - 1.0) > 1e-9:
        raise ValueError(f"base_top2 sum != 1.0 at venue={venue}")
    if abs(sum(base_top3.values()) - 1.0) > 1e-9:
        raise ValueError(f"base_top3 sum != 1.0 at venue={venue}")

    return base_win, base_top2, base_top3


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

            win_lane = parse_lane(row.get("1着"))
            top3_lanes = [parse_lane(row.get("1着")), parse_lane(row.get("2着")), parse_lane(row.get("3着"))]
            if win_lane is None or any(l is None for l in top3_lanes):
                continue

            races.append(
                {
                    "日付": date,
                    "場名": venue,
                    "レース番号": f"{race_no_int}R",
                    "race_no_int": race_no_int,
                    "win_lane": win_lane,
                    "top3_lanes": top3_lanes,
                }
            )

    races.sort(key=lambda r: r["race_no_int"])
    return races


def build_sequential_rows(
    date: str,
    venue: str,
    races: list[dict],
    base_win: dict[int, float],
    base_top2: dict[int, float],
    base_top3: dict[int, float],
    prior_strength: float,
) -> list[dict]:
    obs_win = {lane: 0 for lane in range(1, 7)}
    obs_top2 = {lane: 0 for lane in range(1, 7)}
    obs_top3 = {lane: 0 for lane in range(1, 7)}
    rows = []

    for i, race in enumerate(races):
        observed_races = i

        post_win = {}
        post_top2 = {}
        post_top3 = {}
        m_win = {}
        m_top2 = {}
        m_top3 = {}
        for lane in range(1, 7):
            post_win[lane] = (prior_strength * base_win[lane] + obs_win[lane]) / (prior_strength + observed_races)
            post_top2[lane] = (2 * prior_strength * base_top2[lane] + obs_top2[lane]) / (2 * prior_strength + 2 * observed_races)
            post_top3[lane] = (3 * prior_strength * base_top3[lane] + obs_top3[lane]) / (3 * prior_strength + 3 * observed_races)
            m_win[lane] = post_win[lane] / base_win[lane]
            m_top2[lane] = post_top2[lane] / base_top2[lane]
            m_top3[lane] = post_top3[lane] / base_top3[lane]

        strength_win = sum(abs(post_win[lane] - base_win[lane]) for lane in range(1, 7))
        strength_top2 = sum(abs(post_top2[lane] - base_top2[lane]) for lane in range(1, 7))
        strength_top3 = sum(abs(post_top3[lane] - base_top3[lane]) for lane in range(1, 7))

        row = {
            "日付": date,
            "場名": venue,
            "レース番号": race["レース番号"],
            "observed_races": observed_races,
            "strength_win": strength_win,
            "strength_top2": strength_top2,
            "strength_top3": strength_top3,
        }
        for lane in range(1, 7):
            row[f"m_win_{lane}"] = m_win[lane]
            row[f"m_top2_{lane}"] = m_top2[lane]
            row[f"m_top3_{lane}"] = m_top3[lane]
            row[f"base_win_{lane}"] = base_win[lane]
            row[f"base_top2_{lane}"] = base_top2[lane]
            row[f"base_top3_{lane}"] = base_top3[lane]
            row[f"post_win_{lane}"] = post_win[lane]
            row[f"post_top2_{lane}"] = post_top2[lane]
            row[f"post_top3_{lane}"] = post_top3[lane]
            row[f"obs_win_{lane}"] = obs_win[lane]
            row[f"obs_top2_{lane}"] = obs_top2[lane]
            row[f"obs_top3_{lane}"] = obs_top3[lane]
        rows.append(row)

        obs_win[race["win_lane"]] += 1
        for lane in race["top3_lanes"][:2]:
            obs_top2[lane] += 1
        for lane in race["top3_lanes"]:
            obs_top3[lane] += 1

    return rows


def print_rows(rows: list[dict]) -> None:
    if not rows:
        print("[WARN] no rows to display")
        return

    header = [
        "日付",
        "場名",
        "レース番号",
        "observed_races",
        *(f"m_win_{i}" for i in range(1, 7)),
        *(f"m_top2_{i}" for i in range(1, 7)),
        *(f"m_top3_{i}" for i in range(1, 7)),
        "strength_win",
        "strength_top2",
        "strength_top3",
    ]
    print(",".join(header))
    for row in rows:
        values = [
            row["日付"],
            row["場名"],
            row["レース番号"],
            str(row["observed_races"]),
            *(f"{row[f'm_win_{i}']:.6f}" for i in range(1, 7)),
            *(f"{row[f'm_top2_{i}']:.6f}" for i in range(1, 7)),
            *(f"{row[f'm_top3_{i}']:.6f}" for i in range(1, 7)),
            f"{row['strength_win']:.6f}",
            f"{row['strength_top2']:.6f}",
            f"{row['strength_top3']:.6f}",
        ]
        print(",".join(values))


def write_rows_csv(out_csv: str, rows: list[dict]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    fieldnames = [
        "日付",
        "場名",
        "レース番号",
        "observed_races",
        *(f"m_win_{i}" for i in range(1, 7)),
        *(f"m_top2_{i}" for i in range(1, 7)),
        *(f"m_top3_{i}" for i in range(1, 7)),
        "strength_win",
        "strength_top2",
        "strength_top3",
        *(f"base_win_{i}" for i in range(1, 7)),
        *(f"base_top2_{i}" for i in range(1, 7)),
        *(f"base_top3_{i}" for i in range(1, 7)),
        *(f"post_win_{i}" for i in range(1, 7)),
        *(f"post_top2_{i}" for i in range(1, 7)),
        *(f"post_top3_{i}" for i in range(1, 7)),
        *(f"obs_win_{i}" for i in range(1, 7)),
        *(f"obs_top2_{i}" for i in range(1, 7)),
        *(f"obs_top3_{i}" for i in range(1, 7)),
    ]
    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Demo sequential course-signal regime update from payouts.")
    parser.add_argument("--date", required=True, help="target date (YYYYMMDD)")
    parser.add_argument("--venue", required=True, help="target venue name")
    parser.add_argument("--prior-strength", type=float, default=20.0, help="prior strength K in race-count unit")
    parser.add_argument("--payout-csv", default="data/processed/payouts/all_payout_results.csv", help="payout CSV path")
    parser.add_argument(
        "--prior-csv",
        default="data/priors/payouts_regime/course_signals_latest.csv",
        help="prior CSV path",
    )
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
    if args.prior_strength <= 0:
        raise ValueError("prior-strength must be > 0")

    base_win, base_top2, base_top3 = load_prior(prior_csv, args.venue)
    races = load_day_races(payout_csv, args.date, args.venue)
    if not races:
        print(f"[WARN] no races found for date={args.date}, venue={args.venue}")
        return

    rows = build_sequential_rows(args.date, args.venue, races, base_win, base_top2, base_top3, args.prior_strength)
    print_rows(rows)

    if out_csv:
        write_rows_csv(out_csv, rows)
        print(f"[OK] wrote demo CSV: {out_csv}")


if __name__ == "__main__":
    main()
