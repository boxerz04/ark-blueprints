# -*- coding: utf-8 -*-
"""profile ごとの odds 出力先を解決する小さな設定層。"""

from __future__ import annotations

import os
from dataclasses import dataclass

SUPPORTED_PROFILES = {"5m": 5, "2m": 2}


@dataclass(frozen=True)
class OddsProfilePaths:
    profile: str
    mins_before: int
    odds3t_dir: str
    odds2tf_dir: str
    status_dir: str
    logs_dir: str

    def odds3t_date_dir(self, date: str) -> str:
        return os.path.join(self.odds3t_dir, self.profile, date)

    def odds2tf_date_dir(self, date: str) -> str:
        return os.path.join(self.odds2tf_dir, self.profile, date)

    def status_csv_path(self, date: str) -> str:
        return os.path.join(self.status_dir, self.profile, f"{date}_odds_status_recheck.csv")

    def scheduler_log_path(self) -> str:
        return os.path.join(self.logs_dir, self.profile, "run_odds_scheduler_recheck.log")


def resolve_odds_profile_paths(root_dir: str, profile: str, mins_before: int) -> OddsProfilePaths:
    p = (profile or "").strip().lower()
    if p not in SUPPORTED_PROFILES:
        raise ValueError(f"unsupported profile: {profile}. use one of: {', '.join(sorted(SUPPORTED_PROFILES))}")

    expected = SUPPORTED_PROFILES[p]
    if int(mins_before) != expected:
        raise ValueError(f"profile={p} requires mins_before={expected}, but got {mins_before}")

    return OddsProfilePaths(
        profile=p,
        mins_before=expected,
        odds3t_dir=os.path.join(root_dir, "data", "html", "odds3t"),
        odds2tf_dir=os.path.join(root_dir, "data", "html", "odds2tf"),
        status_dir=os.path.join(root_dir, "data", "odds_status"),
        logs_dir=os.path.join(root_dir, "logs"),
    )
