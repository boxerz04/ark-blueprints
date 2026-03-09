import argparse
import csv
import os


def resolve_path(project_root: str, path: str) -> str:
    return path if os.path.isabs(path) else os.path.join(project_root, path)


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and parent != ".":
        os.makedirs(parent, exist_ok=True)


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


def parse_int(value) -> int | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith('.0'):
        s = s[:-2]
    if not s.isdigit():
        return None
    return int(s)


def parse_bool(value) -> bool:
    s = str(value).strip().lower()
    return s in {"1", "true", "t", "yes", "y"}


def load_prior_by_venue(prior_csv: str) -> dict[str, dict[str, dict[int, float]]]:
    prior_by_venue: dict[str, dict[str, dict[int, float]]] = {}
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
            venue = str(row.get("場名", "")).strip()
            if not venue:
                continue
            base_win: dict[int, float] = {}
            base_top2: dict[int, float] = {}
            base_top3: dict[int, float] = {}
            for lane in range(1, 7):
                base_win[lane] = float(row[f"base_win_{lane}"])
                base_top2[lane] = float(row[f"base_top2_{lane}"])
                base_top3[lane] = float(row[f"base_top3_{lane}"])

            if abs(sum(base_win.values()) - 1.0) > 1e-9:
                raise ValueError(f"base_win sum != 1.0 at venue={venue}")
            if abs(sum(base_top2.values()) - 1.0) > 1e-9:
                raise ValueError(f"base_top2 sum != 1.0 at venue={venue}")
            if abs(sum(base_top3.values()) - 1.0) > 1e-9:
                raise ValueError(f"base_top3 sum != 1.0 at venue={venue}")

            prior_by_venue[venue] = {
                "base_win": base_win,
                "base_top2": base_top2,
                "base_top3": base_top3,
            }

    if not prior_by_venue:
        raise ValueError("no valid prior rows")
    return prior_by_venue


def load_target_races(payout_csv: str, start_date: str, end_date: str) -> list[dict]:
    races = []
    with open(payout_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"日付", "場名", "code", "R", "race_id", "is_valid_result_row", "1着", "2着", "3着"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise KeyError(f"payout CSV missing required columns: {sorted(missing)}")

        for row in reader:
            date = str(row.get("日付", "")).strip()
            if not date:
                continue
            if date < start_date or date > end_date:
                continue

            venue = str(row.get("場名", "")).strip()
            code = parse_int(row.get("code"))
            race_no = parse_int(row.get("R"))
            race_id = str(row.get("race_id", "")).strip()
            if not venue or code is None or race_no is None or not race_id:
                continue

            races.append(
                {
                    "date": date,
                    "venue": venue,
                    "code": code,
                    "R": race_no,
                    "race_id": race_id,
                    "is_valid_result_row": parse_bool(row.get("is_valid_result_row")),
                    "rank1": parse_lane(row.get("1着")),
                    "rank2": parse_lane(row.get("2着")),
                    "rank3": parse_lane(row.get("3着")),
                }
            )

    races.sort(key=lambda r: (r["date"], r["code"], r["R"], r["race_id"]))
    return races


def build_features(races: list[dict], prior_by_venue: dict[str, dict[str, dict[int, float]]], prior_strength: float) -> list[dict]:
    state_by_day_venue: dict[tuple[str, str], dict[str, dict[int, int]]] = {}
    rows: list[dict] = []

    for race in races:
        venue = race["venue"]
        date = race["date"]
        key = (date, venue)

        if venue not in prior_by_venue:
            continue

        base_win = prior_by_venue[venue]["base_win"]
        base_top2 = prior_by_venue[venue]["base_top2"]
        base_top3 = prior_by_venue[venue]["base_top3"]

        if key not in state_by_day_venue:
            state_by_day_venue[key] = {
                "obs_win": {lane: 0 for lane in range(1, 7)},
                "obs_top2": {lane: 0 for lane in range(1, 7)},
                "obs_top3": {lane: 0 for lane in range(1, 7)},
                "observed_races": 0,
            }
        state = state_by_day_venue[key]

        observed_races = state["observed_races"]
        post_win = {}
        post_top2 = {}
        post_top3 = {}
        m_win = {}
        m_top2 = {}
        m_top3 = {}
        for lane in range(1, 7):
            post_win[lane] = (prior_strength * base_win[lane] + state["obs_win"][lane]) / (prior_strength + observed_races)
            post_top2[lane] = (2 * prior_strength * base_top2[lane] + state["obs_top2"][lane]) / (
                2 * prior_strength + 2 * observed_races
            )
            post_top3[lane] = (3 * prior_strength * base_top3[lane] + state["obs_top3"][lane]) / (
                3 * prior_strength + 3 * observed_races
            )
            m_win[lane] = post_win[lane] / base_win[lane]
            m_top2[lane] = post_top2[lane] / base_top2[lane]
            m_top3[lane] = post_top3[lane] / base_top3[lane]

        out_row = {
            "race_id": race["race_id"],
            "date": race["date"],
            "code": race["code"],
            "R": race["R"],
            "strength_win": sum(abs(post_win[lane] - base_win[lane]) for lane in range(1, 7)),
            "strength_top2": sum(abs(post_top2[lane] - base_top2[lane]) for lane in range(1, 7)),
            "strength_top3": sum(abs(post_top3[lane] - base_top3[lane]) for lane in range(1, 7)),
        }
        for lane in range(1, 7):
            out_row[f"m_win_{lane}"] = m_win[lane]
            out_row[f"m_top2_{lane}"] = m_top2[lane]
            out_row[f"m_top3_{lane}"] = m_top3[lane]
        rows.append(out_row)

        if race["is_valid_result_row"] and race["rank1"] is not None and race["rank2"] is not None and race["rank3"] is not None:
            state["obs_win"][race["rank1"]] += 1
            state["obs_top2"][race["rank1"]] += 1
            state["obs_top2"][race["rank2"]] += 1
            state["obs_top3"][race["rank1"]] += 1
            state["obs_top3"][race["rank2"]] += 1
            state["obs_top3"][race["rank3"]] += 1
            state["observed_races"] += 1

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build race_id-level regime feature CSV from payout results.")
    parser.add_argument("--start-date", required=True, help="start date (YYYYMMDD)")
    parser.add_argument("--end-date", required=True, help="end date (YYYYMMDD)")
    parser.add_argument("--prior-csv", required=True, help="course signals prior CSV path")
    parser.add_argument("--payout-csv", required=True, help="parsed payout results CSV path")
    parser.add_argument("--out-csv", required=True, help="output CSV path")
    parser.add_argument("--prior-strength", type=float, default=20.0, help="prior strength K in race-count unit")
    args = parser.parse_args()

    if args.start_date > args.end_date:
        raise ValueError("start-date must be <= end-date")
    if args.prior_strength <= 0:
        raise ValueError("prior-strength must be > 0")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)

    prior_csv = resolve_path(project_root, args.prior_csv)
    payout_csv = resolve_path(project_root, args.payout_csv)
    out_csv = resolve_path(project_root, args.out_csv)

    if not os.path.exists(prior_csv):
        raise FileNotFoundError(f"prior CSV not found: {prior_csv}")
    if not os.path.exists(payout_csv):
        raise FileNotFoundError(f"payout CSV not found: {payout_csv}")

    prior_by_venue = load_prior_by_venue(prior_csv)
    races = load_target_races(payout_csv, args.start_date, args.end_date)
    rows = build_features(races, prior_by_venue, args.prior_strength)

    fieldnames = [
        "race_id",
        "date",
        "code",
        "R",
        *(f"m_win_{i}" for i in range(1, 7)),
        *(f"m_top2_{i}" for i in range(1, 7)),
        *(f"m_top3_{i}" for i in range(1, 7)),
        "strength_win",
        "strength_top2",
        "strength_top3",
    ]

    ensure_parent_dir(out_csv)
    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] wrote regime feature CSV: {out_csv} (rows={len(rows)})")


if __name__ == "__main__":
    main()
