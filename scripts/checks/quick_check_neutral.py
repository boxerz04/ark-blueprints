# scripts/checks/quick_check_neutral.py
from src.raceinfo_features import get_point, ranking_point_map, condition_point_map
import pandas as pd

row_F = pd.Series({"entry_history":"1", "rank_history":"F"})  # 1コースでF
row_L = pd.Series({"entry_history":"5", "rank_history":"L"})  # 5コースでL

print("ranking F:", get_point(row_F, ranking_point_map))      # 期待: [0]
print("ranking L:", get_point(row_L, ranking_point_map))      # 期待: [0]
print("condition F:", get_point(row_F, condition_point_map))  # 期待: [0]
print("condition L:", get_point(row_L, condition_point_map))  # 期待: [0]
