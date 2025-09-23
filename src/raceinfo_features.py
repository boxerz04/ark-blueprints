# -*- coding: utf-8 -*-
"""
src/raceinfo_features.py

出走表（racelist）の HTML(.bin) から「今節スナップショット」特徴量を抽出する関数群。
- ここでは “関数のみ” を提供。CSV保存などの I/O は呼び出し側で行う。
- 元のノートブック実装のロジックをできる限り維持（=後方互換）しつつ、コメントを厚めに付与。
- HTML のレイアウト依存（CSS セレクタ / read_html の前提）は現状維持。堅牢化は別対応。

主な公開関数:
- parse_racelist_html(content: bytes) -> pd.DataFrame
    ※ 正式名称。旧名 process_racelist_content() は互換エイリアス。
- calculate_raceinfo_points(raceinfo: pd.DataFrame, ...) -> pd.DataFrame
- process_all_files_in_directory(directory: str, ...) -> pd.DataFrame
- load_html(file_path: str) -> bytes
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup


# ======================================================================================
# スコアマップ（元コード準拠）
# ======================================================================================

#: 進入(1-6)×着順(1-6)で与える「ランキングポイント」
ranking_point_map: Dict[int, Dict[int, int]] = {
    1: {1: 0, 2: -1, 3: -2, 4: -3, 5: -4, 6: -5},
    2: {1: 1, 2: 0, 3: -1, 4: -2, 5: -3, 6: -4},
    3: {1: 2, 2: 1, 3: 0, 4: -1, 5: -2, 6: -3},
    4: {1: 3, 2: 2, 3: 1, 4: 0, 5: -1, 6: -2},
    5: {1: 4, 2: 3, 3: 2, 4: 1, 5: 0, 6: -1},
    6: {1: 5, 2: 4, 3: 3, 4: 2, 5: 1, 6: 0},
}

#: 進入(1-6)×着順(1-6)で与える「コンディションポイント」
condition_point_map: Dict[int, Dict[int, int]] = {
    1: {1: 2, 2: -1, 3: -2, 4: -3, 5: -4, 6: -5},
    2: {1: 1, 2: 1, 3: -1, 4: -2, 5: -3, 6: -4},
    3: {1: 2, 2: 1, 3: 1, 4: -1, 5: -2, 6: -3},
    4: {1: 3, 2: 2, 3: 1, 4: 0, 5: -1, 6: -2},
    5: {1: 4, 2: 3, 3: 2, 4: 1, 5: 0, 6: -1},
    6: {1: 5, 2: 4, 3: 3, 4: 2, 5: 1, 6: -1},
}


# ======================================================================================
# 小さなユーティリティ（元コードの挙動を尊重）
# ======================================================================================

def add_zero(x: Any) -> Any:
    """
    元実装: 文字列であれば先頭に '0' を付与。
    本来は「'.12' → '0.12'」だけに適用するのが安全だが、互換性のため仕様維持。
    """
    if isinstance(x, str):
        return '0' + x
    return x


def is_float(x: Any) -> bool:
    """float に変換可能かどうかを簡易判定（例外は False）。"""
    try:
        float(x)
        return True
    except Exception:
        return False


def convert_to_float(x: Any) -> float:
    """
    float 変換。失敗時は 0.0（統計的には NaN の方が安全だが、元仕様を優先）。
    """
    if is_float(x):
        return float(x)
    return 0.0


def assign(x: Any, point_map: Optional[Dict[int, int]] = None) -> int:
    """
    着順(1-6)を素点(10,8,6,4,2,1)に変換（元仕様）。
    """
    if point_map is None:
        point_map = {1: 10, 2: 8, 3: 6, 4: 4, 5: 2, 6: 1}
    try:
        return point_map.get(int(x), 0)
    except Exception:
        return 0


def get_point(row: pd.Series, p: Dict[int, Dict[int, int]]) -> list[int]:
    """
    entry_history と rank_history からポイント配列を作る。
    厳密ニュートラル: rankが数字でない（F/L/欠など）は 0 点を加える。
    """
    pts: list[int] = []
    eh: str = row["entry_history"]
    rh: str = row["rank_history"]

    # zipで安全にペアリング（長さ不一致でも短い方に合わせる）
    for x_ch, y_ch in zip(eh, rh):
        if not x_ch.isdigit():
            continue  # 進入が数字でないなら無視
        x = int(x_ch)

        if y_ch.isdigit():
            y = int(y_ch)
            pts.append(p.get(x, {}).get(y, 0))
        else:
            # ★ここがキモ：F/L/欠などは「0点（ニュートラル）」を加える
            pts.append(0)

    return pts


def sum_point(row: pd.Series, col: str) -> int:
    """ポイント列（List[int]）の合計値を返す。"""
    return sum(map(int, row[col]))


def join_point(row: pd.Series, col: str) -> str:
    """ポイント列（List[int]）をスペース区切りの文字列に連結。"""
    return " ".join(map(str, row[col]))


def load_html(file_path: str) -> bytes:
    """保存された .bin(HTML) を読み込み、bytes を返すだけの小関数。"""
    with open(file_path, 'rb') as file:
        return file.read()


# ======================================================================================
# コア処理: HTML bytes → “今節スナップショット” DataFrame
# ======================================================================================

def parse_racelist_html(content: bytes) -> pd.DataFrame:
    """
    racelist(出走表) HTML の bytes から、今節スナップショット特徴量を抽出して DataFrame を返す。

    戻り値の主な列（元ノートブック準拠／順序は概ねこの並び）:
      - player_id
      - ST_timing
      - ST_mean_current
      - ST_rank_current
      - ST_previous_time
      - boat_color
      - entry_history
      - race_ct_current
      - rank_history
      - score
      - score_rate

    注記:
    - pandas.read_html と CSS セレクタに強く依存（= レイアウト変更に弱い）。ここでは現状維持。
    - リーク対策（as-of 切断）は呼び出し側で担保（この関数は“あり物の表”をそのまま集約）。
    """
    # 1) HTML を DataFrame 群として読み込む
    df_list = pd.read_html(content)
    # 2) BeautifulSoup でもパース（player_id / boat_color 取得に使用）
    soup = BeautifulSoup(content, 'html.parser')

    # --- player_id 抽出（div.is-fs11 に「登録番号 名前」等がある前提）---
    div_tags = soup.find_all('div', class_='is-fs11')
    numbers = [div.get_text(strip=True).split()[0] for div in div_tags]
    even_index_numbers = numbers[0::2]  # 偶数番だけ取り出す元実装
    player = pd.DataFrame(even_index_numbers, columns=['player_id'])

    # --- 枠番色 boat_color 抽出（CSSクラス 'boatColor{1..6}' を拾う想定）---
    selector_1 = 'body > main > div > div > div > div.contentsFrame1_inner > div.table1.is-tableFixed__3rdadd > table'
    boatNo: List[List[str]] = []
    table = soup.select_one(selector_1)
    for i in range(4, 10):  # tbody:nth-child(24..29) を辿る元実装
        selector_2 = (
            'body > main > div > div > div > div.contentsFrame1_inner > '
            f'div.table1.is-tableFixed__3rdadd > table > tbody:nth-child(2{i}) > tr:nth-child(1)'
        )
        cell = table.select(selector_2) if table else []
        cell_str = str(cell)
        matches = re.findall(r'boatColor(\d+)', cell_str)
        if matches:
            matches.pop(0)  # 先頭要素はヘッダ由来を想定し除去（元実装）
            boatNo.append([''.join(matches)])
    boatColor = pd.DataFrame(boatNo, columns=['boat_color'])

    # --- 今節成績“右側ブロック”（元実装）---
    # MultiIndex を2段剥がし、対象カラム(9:23)を抽出
    record = df_list[1].droplevel(0, axis=1).droplevel(0, axis=1).iloc[:, 9:23]

    # (A) 進入履歴 & 今節出走数
    entry = record[1::4].copy()
    entry['entry_history'] = entry.fillna('').apply(lambda x: ''.join(x), axis=1)
    entry['race_ct_current'] = entry.iloc[:, :-1].count(axis=1)
    entry.reset_index(inplace=True, drop=True)
    entry = entry.iloc[:, -2:]  # ['entry_history', 'race_ct_current']

    # (B) F/L/平均ST 表（平均ST順位はここで計算するが、最終出力には使っていない＝元実装踏襲）
    flst = df_list[1].droplevel(0, axis=1).droplevel(0, axis=1).iloc[:, 3:4]
    flst = flst['F数 L数 平均ST'].str.split(' ', expand=True).drop([1, 3], axis=1)
    flst[0] = flst[0].str.replace('F', '')
    flst[2] = flst[2].str.replace('L', '')
    flst.columns = ['F', 'L', '平均ST']
    flst = flst[::4]
    flst['平均ST'] = flst['平均ST'].str.replace('-', '0.19')  # “-” を 0.19 とみなす元仕様
    flst.reset_index(inplace=True, drop=True)
    flst['平均ST順位'] = pd.to_numeric(flst['平均ST']).rank(ascending=True, method='min')

    # (C) ST 履歴・平均・直前ST（文字列4桁）
    st = record[2::4].map(add_zero)  # 文字列なら先頭に '0' を付ける元仕様（厳密性より互換優先）
    st.reset_index(inplace=True, drop=True)
    st['ST_timing'] = st.fillna('').apply(lambda x: ''.join(x), axis=1)
    st['ST_mean_current'] = st.fillna('').iloc[:, :-1].map(convert_to_float).sum(axis=1) / entry['race_ct_current']
    st['ST_rank_current'] = pd.to_numeric(st['ST_mean_current']).rank(ascending=True, method='min')
    st['ST_previous_time'] = st.iloc[:, :-2].fillna('').apply(lambda x: ''.join(x)[-4:], axis=1)
    st = st.iloc[:, -4:]  # ['ST_timing','ST_mean_current','ST_rank_current','ST_previous_time']

    # (D) 着順履歴（全角→半角）
    result = record[3::4].copy()
    result['rank_history'] = result.fillna('').apply(lambda x: ''.join(x), axis=1)
    result.reset_index(inplace=True, drop=True)
    table_map = {chr(0xFF01 + i): chr(0x21 + i) for i in range(94)}  # 全角→半角
    result['rank_history'] = result['rank_history'].apply(lambda x: x.translate(table_map))

    # 着順を素点(10,8,6,4,2,1)化 → 合計 → 出走数で割って rate
    point = record[3::4].map(convert_to_float).map(assign)
    point.reset_index(inplace=True, drop=True)
    result['score'] = point.sum(axis=1)
    result = result.iloc[:, -2:]  # ['rank_history','score']
    result['score_rate'] = result['score'] / entry['race_ct_current']

    # 最終結合（列順は元実装の流れに合わせる）
    raceinfo = pd.concat([player, st, boatColor, entry, result], axis=1)
    return raceinfo


# 互換エイリアス（旧名）：将来的に削除してOK
def process_racelist_content(content: bytes) -> pd.DataFrame:  # TODO: deprecate
    """旧API名。内部で parse_racelist_html() を呼ぶだけ。"""
    return parse_racelist_html(content)


# ======================================================================================
# ポイント列の付与（ランキング/コンディション）
# ======================================================================================

def calculate_raceinfo_points(
    raceinfo: pd.DataFrame,
    ranking_map: Dict[int, Dict[int, int]] = ranking_point_map,
    condition_map: Dict[int, Dict[int, int]] = condition_point_map,
    race_id: Optional[str] = None,
) -> pd.DataFrame:
    """
    parse_racelist_html() の出力に対し、ランキング/コンディション・ポイント系の列を付与。
    追加列:
      - ranking_point            : 各走のポイント（スペース区切り文字列）
      - ranking_point_sum        : 合計
      - ranking_point_rate       : 合計 / 出走数
      - condition_point          : 各走のポイント（スペース区切り文字列）
      - condition_point_sum      : 合計
      - condition_point_rate     : 合計 / 出走数
      - race_id                  : 任意。指定時のみ付与。
    """
    raceinfo['ranking_point'] = raceinfo.apply(get_point, p=ranking_map, axis=1)
    raceinfo["ranking_point_sum"] = raceinfo.apply(sum_point, col='ranking_point', axis=1)
    raceinfo["ranking_point"] = raceinfo.apply(join_point, col='ranking_point', axis=1)
    raceinfo["ranking_point_rate"] = raceinfo["ranking_point_sum"] / raceinfo['race_ct_current']

    raceinfo["condition_point"] = raceinfo.apply(get_point, p=condition_map, axis=1)
    raceinfo["condition_point_sum"] = raceinfo.apply(sum_point, col='condition_point', axis=1)
    raceinfo["condition_point"] = raceinfo.apply(join_point, col='condition_point', axis=1)
    raceinfo["condition_point_rate"] = raceinfo["condition_point_sum"] / raceinfo['race_ct_current']

    if race_id is not None:
        raceinfo['race_id'] = race_id

    return raceinfo



# ======================================================================================
# ディレクトリ内 .bin を一括処理（※保存は呼び出し側で実施）
# ======================================================================================

def process_all_files_in_directory(
    directory: str,
    ranking_map: Dict[int, Dict[int, int]] = ranking_point_map,
    condition_map: Dict[int, Dict[int, int]] = condition_point_map,
) -> pd.DataFrame:
    """
    指定ディレクトリの .bin(HTML) を全走査し、raceinfo DataFrame を縦結合して返す。
    - CSV 保存などは呼び出し側で実施する方針。
    - race_id はファイル名から連続数字を抽出（なければベース名）。
    """
    all_raceinfo: List[pd.DataFrame] = []
    bin_files = [f for f in os.listdir(directory) if f.endswith('.bin')]

    for file_name in bin_files:
        file_path = os.path.join(directory, file_name)
        racelist_content = load_html(file_path)

        # 1ファイル分をパース
        raceinfo = parse_racelist_html(racelist_content)

        # race_id をファイル名から抽出
        m = re.search(r'\d+', file_name)
        race_id = m.group() if m else os.path.splitext(file_name)[0]

        # ポイント列を追加
        raceinfo = calculate_raceinfo_points(raceinfo, ranking_map, condition_map, race_id)
        all_raceinfo.append(raceinfo)

    if all_raceinfo:
        return pd.concat(all_raceinfo, ignore_index=True)
    return pd.DataFrame()
 
