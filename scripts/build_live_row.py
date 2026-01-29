# -*- coding: utf-8 -*-
"""
scripts/build_live_row.py
本番直前に 1 レース分の “raw 相当の 6 行 DataFrame” を生成（raceresult は参照しない）

使い方:
  python scripts/build_live_row.py --date 20250903 --jcd 12 --race 3 --online \
      --out data/live/raw_20250903_12_03.csv

仕様:
- 参照: pay / index / racelist / pcexpect / beforeinfo / raceindex（raceresultは使わない）
- --online 指定で公式から取得し、**data/live/html/** 配下にキャッシュ（kind ごとのフォルダ）
- ローカル読込は **data/live/html → data/html** の順で探索
- 出力は 6 行（1～6艇）固定。race_id = YYYYMMDD + jcd2桁 + R2桁
- 将来のラグ特徴用に、ライブ出力にも空列の `entry` / `is_wakunari` を追加（Int64, 全て NA）
- 学習raw互換のため `wakuban`（枠番）を beforeinfo から作成（無い場合は1..6で補完）
- 節ID（section_id）は schedule の開始日を優先して生成（build_raw_csv.py と同一仕様）。
- 列順はデフォルトで data/raw/ の「最新 *_raw.csv」に自動整列（足りない列はNA追加・余分は削除）
"""

import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import requests  # robust read_html のフォールバックで使用
import warnings
try:
    from bs4.builder import XMLParsedAsHTMLWarning
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
except Exception:
    pass


# ================= ルート基準のパスヘルパ =================
root_dir = os.path.dirname(os.path.dirname(__file__))

def data_path(*parts) -> str:
    """プロジェクトルート/data/ 以下への絶対パスを生成"""
    return os.path.join(root_dir, "data", *parts)

# 保存先のベース
LIVE_HTML_BASE = ("live", "html")   # ← 新設（オンライン取得のキャッシュ先）
TRAIN_HTML_BASE = ("html",)         # 既存の学習用HTMLの親（サブは各kindで付ける）

# =============== HTML サブフォルダ定義 ===============
HTML_SUBDIRS = {
    "pay":        ("pay",),
    "index":      ("index",),
    "racelist":   ("racelist",),
    "pcexpect":   ("pcexpect",),
    "beforeinfo": ("beforeinfo",),
    "raceindex":  ("raceindex",),
}

# 重要: rno= を使用（racenum= ではない）
URLS = {
    "pay":        lambda hd: f"https://www.boatrace.jp/owpc/pc/race/pay?hd={hd}",
    "index":      lambda hd: f"https://www.boatrace.jp/owpc/pc/race/index?hd={hd}",
    "racelist":   lambda hd,jcd,r: f"https://www.boatrace.jp/owpc/pc/race/racelist?hd={hd}&jcd={str(jcd).zfill(2)}&rno={int(r)}",
    "pcexpect":   lambda hd,jcd,r: f"https://www.boatrace.jp/owpc/pc/race/pcexpect?hd={hd}&jcd={str(jcd).zfill(2)}&rno={int(r)}",
    "beforeinfo": lambda hd,jcd,r: f"https://www.boatrace.jp/owpc/pc/race/beforeinfo?hd={hd}&jcd={str(jcd).zfill(2)}&rno={int(r)}",
    "raceindex":  lambda hd,jcd:   f"https://www.boatrace.jp/owpc/pc/race/raceindex?hd={hd}&jcd={str(jcd).zfill(2)}",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ark-live/1.4)"}

def ensure_dir(path: str) -> None:
    Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)

def normalize_yyyymmdd(s: str) -> str:
    s = s.replace("-", "").strip()
    if re.fullmatch(r"\d{8}", s):
        return s
    raise ValueError("--date は YYYYMMDD 形式で指定してください。")

def compute_section_id_from_schedule(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """
    schedule の開始日を優先して section_id を作る（ベクトル化）。
    section_id = <節開始日YYYYMMDD>_<場コード2桁>

    schedule が欠損/壊れて開始日が取れない行は date にフォールバックする。
    戻り値:
      - section_id（Series[str]）
      - fallback（Series[bool]）: dateフォールバックした行

    NOTE:
    - 日次raw（build_raw_csv.py）と **同一仕様**。
    - 推論/ライブでも section_id を統一することで、motor_section_features の
      (motor_id, section_id) JOIN が学習時と一致し、節途中日でも結合が崩れない。
    """
    # 必須列チェック
    for col in ("date", "code", "schedule"):
        if col not in df.columns:
            raise KeyError(f"compute_section_id_from_schedule: 必須列 '{col}' がありません")

    date_s = df["date"].astype(str).str.strip()
    code2 = df["code"].astype(str).str.strip().str.zfill(2)
    sched = df["schedule"].astype(str).str.strip()

    m = sched.str.extract(r"^\s*(\d{1,2})/(\d{1,2})\s*-\s*(\d{1,2})/(\d{1,2})\s*$")
    start_m = pd.to_numeric(m[0], errors="coerce")
    start_d = pd.to_numeric(m[1], errors="coerce")

    year = pd.to_numeric(date_s.str.slice(0, 4), errors="coerce")
    proc_month = pd.to_numeric(date_s.str.slice(4, 6), errors="coerce")

    fallback = start_m.isna() | start_d.isna() | year.isna()

    # 年跨ぎ補正：処理日が1月/2月で、schedule開始月が12月なら前年開始
    year_adj = year.copy()
    mask_cross = (~fallback) & (proc_month.isin([1, 2])) & (start_m == 12)
    year_adj.loc[mask_cross] = year_adj.loc[mask_cross] - 1

    start_mm = start_m.fillna(1).astype(int).astype(str).str.zfill(2)
    start_dd = start_d.fillna(1).astype(int).astype(str).str.zfill(2)
    start_yyyymmdd = year_adj.fillna(0).astype(int).astype(str) + start_mm + start_dd

    # フォールバックは date を使う
    start_yyyymmdd = start_yyyymmdd.mask(fallback, date_s)

    section_id = start_yyyymmdd + "_" + code2
    return section_id, fallback


def live_html_path(kind: str, date: str, jcd: Optional[str]=None, rno: Optional[str]=None) -> str:
    """ライブ用キャッシュファイルの保存パスを返す"""
    if kind in ("pay", "index"):
        fname = f"{kind}{date}.bin"
    elif kind in ("racelist", "pcexpect", "beforeinfo"):
        race_id = f"{date}{str(jcd).zfill(2)}{str(rno).zfill(2)}"
        fname = f"{kind}{race_id}.bin"
    elif kind == "raceindex":
        fname = f"{kind}{date}{str(jcd).zfill(2)}.bin"
    else:
        raise ValueError(f"unknown kind: {kind}")
    return data_path(*LIVE_HTML_BASE, *HTML_SUBDIRS[kind], fname)

def train_html_path(kind: str, date: str, jcd: Optional[str]=None, rno: Optional[str]=None) -> str:
    """学習用HTML（従来フォルダ）のファイルパスを返す"""
    if kind in ("pay", "index"):
        fname = f"{kind}{date}.bin"
    elif kind in ("racelist", "pcexpect", "beforeinfo"):
        race_id = f"{date}{str(jcd).zfill(2)}{str(rno).zfill(2)}"
        fname = f"{kind}{race_id}.bin"
    elif kind == "raceindex":
        fname = f"{kind}{date}{str(jcd).zfill(2)}.bin"
    else:
        raise ValueError(f"unknown kind: {kind}")
    return data_path(*TRAIN_HTML_BASE, *HTML_SUBDIRS[kind], fname)

def load_from_file(path: str) -> Optional[BeautifulSoup]:
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        html = f.read().decode("utf-8", errors="ignore")
    return BeautifulSoup(html, "html.parser")

def load_local(kind: str, date: str, jcd: Optional[str]=None, rno: Optional[str]=None) -> Optional[BeautifulSoup]:
    """ローカル読込は live → train の順でフォールバック"""
    # live 側
    s = load_from_file(live_html_path(kind, date, jcd, rno))
    if s is not None:
        return s
    # 学習側
    return load_from_file(train_html_path(kind, date, jcd, rno))

def fetch_online(kind: str, date: str, jcd: Optional[str]=None, rno: Optional[str]=None) -> Optional[BeautifulSoup]:
    """オンライン取得し、live キャッシュに保存"""
    if kind in ("pay", "index"):
        url = URLS[kind](date)
    elif kind in ("racelist", "pcexpect", "beforeinfo"):
        if jcd is None or rno is None:
            return None
        url = URLS[kind](date, jcd, rno)
    elif kind == "raceindex":
        if jcd is None:
            return None
        url = URLS[kind](date, jcd)
    else:
        return None

    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        res.raise_for_status()
        html_text = res.text
        # live 側にキャッシュ
        save_path = live_html_path(kind, date, jcd, rno)
        ensure_dir(save_path)
        with open(save_path, "wb") as f:
            f.write(html_text.encode("utf-8", errors="ignore"))
        return BeautifulSoup(html_text, "html.parser")
    except Exception:
        return None

def get_soup(kind: str, online: bool, date: str, jcd: Optional[str]=None, rno: Optional[str]=None) -> Optional[BeautifulSoup]:
    if online:
        s = fetch_online(kind, date, jcd, rno)  # 取得＋liveキャッシュ
        if s is not None:
            return s
    # オンライン失敗時やオフライン時はローカルから
    return load_local(kind, date, jcd, rno)

# =============== robust read_html ===============
def read_html_tables_robust(html_or_url: str):
    """
    pandas.read_html の堅いラッパー:
      1) StringIO + flavor='lxml'
      2) 失敗時: flavor='html5lib'
      3) 入力がURLだった場合はGETして再挑戦
    """
    is_url = bool(re.match(r"^https?://", html_or_url))
    text = html_or_url
    try:
        return pd.read_html(StringIO(text), flavor="lxml")
    except Exception:
        try:
            return pd.read_html(StringIO(text), flavor="html5lib")
        except Exception:
            if is_url:
                resp = requests.get(html_or_url, headers=HEADERS, timeout=10)
                resp.raise_for_status()
                try:
                    return pd.read_html(StringIO(resp.text), flavor="lxml")
                except Exception:
                    return pd.read_html(StringIO(resp.text), flavor="html5lib")
            raise

# ======== 学習側と同一ルールの ST 解析 ========
def parse_st(val):
    """
    'F.01' -> -0.01, '0.07' -> +0.07, 'L.03' -> +0.03
    '3  L' / '3F.01' の混入は 'L' / 'F.01' として解釈。その他は NaN。
    """
    if val is None:
        return np.nan
    t = str(val).strip()
    if t == "" or t in {"-", "—", "ー", "―"}:
        return np.nan
    t = t.replace("Ｆ", "F").replace("Ｌ", "L")
    m = re.match(r"^\d+\s*([FL](?:\.\d+)?)$", t, flags=re.I)
    if m:
        t = m.group(1)
    sign = 1.0
    if t[:1].lower() == "f":
        sign, t = -1.0, t[1:].strip()
    elif t[:1].lower() == "l":
        sign, t =  1.0, t[1:].strip()
    if re.fullmatch(r"\d{2}", t):
        t = "0." + t
    if t.startswith("."):
        t = "0" + t
    if t == "" or not re.fullmatch(r"\d+(\.\d+)?", t):
        return np.nan
    try:
        return sign * float(t)
    except ValueError:
        return np.nan

# =============== 解析：pay / index ===============
def parse_pay(soup_pay: BeautifulSoup) -> pd.DataFrame:
    image_tags = soup_pay.find_all('img', alt=True)
    race_tracks = []
    for img in image_tags:
        alt_text = img['alt']
        # 日本語を含む ALT のみ対象
        if re.search(r'[\u3000-\u303F\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', alt_text):
            src = img.get('src', '')
            m = re.search(r'text_place1_(\d+)\.png', src)
            if m:
                track_code = m.group(1)
                race_tracks.append((alt_text, track_code))

    race_data = []
    for venue_name, track_code in race_tracks:
        area_name_tag = soup_pay.find('img', alt=lambda x: x and venue_name.strip() in x.strip())
        if not area_name_tag:
            continue
        parent_div = area_name_tag.find_parent('div', class_='table1_area')

        # グレード
        race_grade = "不明"
        area_type_tag = parent_div.find('p', class_='table1_areaType')
        if area_type_tag:
            for cls in area_type_tag.get('class', []):
                if cls.startswith('is-'):
                    race_grade = cls.replace("is-", "")
                    break

        # タイプ
        race_type = "分類なし"
        area_time_tag = parent_div.find('p', class_='table1_areaTime')
        if area_time_tag:
            for cls in area_time_tag.get('class', []):
                if cls.startswith('is-'):
                    race_type = cls.replace("is-", "")
                    break

        # 属性
        race_attribute = "分類なし"
        area_women_tag = parent_div.find('p', class_='table1_areaWomen')
        if area_women_tag:
            for cls in area_women_tag.get('class', []):
                if cls.startswith('is-'):
                    race_attribute = cls.replace("is-", "")
                    break

        race_data.append((venue_name, track_code, race_grade, race_type, race_attribute))

    return pd.DataFrame(race_data, columns=['place', 'code', 'race_grade', 'race_type', 'race_attribute'])

def parse_index(soup_idx: BeautifulSoup, date: str) -> pd.DataFrame:
    data_idx = []
    rows = soup_idx.find_all("td", class_="is-arrow1")
    for row in rows:
        venue_img = row.find("img")
        venue_name = venue_img["alt"] if venue_img and "alt" in venue_img.attrs else None

        title_td = row.find_next("td", class_="is-alignL is-fBold is-p10-7")
        title = title_td.text.strip() if title_td else None

        date_td = title_td.find_next("td") if title_td else None
        if date_td:
            raw_date_info = date_td.decode_contents().strip()
            date_parts = raw_date_info.split("<br/>")
            schedule = date_parts[0].strip() if len(date_parts) > 0 else None
            day = date_parts[1].strip() if len(date_parts) > 1 else None
        else:
            schedule, day = None, None

        grade_td = row.find_next("td", class_=lambda x: x and x.startswith("is-") and x != "is-p10-10")
        if grade_td:
            race_classes = [cls for cls in grade_td.get("class", []) if cls.startswith('is-')]
            if len(race_classes) == 2:
                race_grade = race_classes[0].replace("is-", "")
                race_attribute = race_classes[1].replace("is-", "")
            elif len(race_classes) == 1:
                race_grade = race_classes[0].replace("is-", "")
                race_attribute = "分類なし"
            else:
                race_grade = "不明"
                race_attribute = "不明"
        else:
            race_grade, race_attribute = "不明", "不明"

        # 会期日数
        duration = None
        if schedule:
            try:
                start_date_str, end_date_str = schedule.split("-")
                current_year = int(date[:4])
                start_date = datetime.strptime(f"{current_year}/{start_date_str.strip()}", "%Y/%m/%d")
                end_date = datetime.strptime(f"{current_year}/{end_date_str.strip()}", "%Y/%m/%d")
                if start_date > end_date:
                    end_date = end_date.replace(year=current_year + 1)
                duration = (end_date - start_date).days + 1
            except Exception:
                duration = None

        # 「○日目」を数値化
        day_number = None
        if day and schedule:
            try:
                if "最終日" in day:
                    day_number = duration
                elif "初日" in day:
                    day_number = 1
                else:
                    day_number = int(''.join(filter(str.isdigit, day)))
            except Exception:
                day_number = None

        if venue_name:
            data_idx.append({
                "place": venue_name,
                "title": title,
                "day": day_number,
                "section": duration,
                "schedule": schedule
            })

    df_index = pd.DataFrame(data_idx)
    if df_index.empty:
        return df_index
    df_index_subset = df_index[["place", "title", "day", "section", "schedule"]]
    # 住之江の二重（電話投票テーブル）など重複は末尾落とし
    if df_index_subset['place'].duplicated().any():
        df_index_subset = df_index_subset[:-1]
    return df_index_subset

# =============== 1レース分の raw（結果なし）を構築 ===============
def build_live_raw(date: str, jcd: str, rno: str, online: bool) -> pd.DataFrame:
    # pay/index
    soup_pay = get_soup("pay", online, date)
    soup_idx = get_soup("index", online, date)
    if soup_pay is None or soup_idx is None:
        raise RuntimeError("pay/index の取得に失敗（--online か .bin を確認）")

    df_pay = parse_pay(soup_pay)
    df_index = parse_index(soup_idx, date)
    race_merged = pd.merge(df_pay, df_index, on="place", how="left")
    race_merged["code"] = race_merged["code"].astype(str).str.zfill(2)

    # racelist
    soup_rl = get_soup("racelist", online, date, jcd, rno)
    racelist_url = URLS["racelist"](date, jcd, rno)
    try:
        tables = read_html_tables_robust(str(soup_rl) if soup_rl is not None else racelist_url)
    except Exception as e:
        raise RuntimeError(f"racelist のテーブル取得に失敗: {e}")

    # === 学習スクリプト準拠のパース ===
    racelist = tables[1].droplevel(0, axis=1).droplevel(0, axis=1)
    racelist1 = racelist.iloc[:, :4]
    racelist1 = racelist1['登録番号/級別 氏名 支部/出身地 年齢/体重'].str.split('/', expand=True)

    team = racelist1[1].apply(lambda x: x.split()[-1])
    origin = racelist1[2].str.split(' ', expand=True)[0]
    ab = racelist1[1].str.split(' ', expand=True)[1]
    age = racelist1[2].str.split('  ', expand=True)[1].str.replace('歳', '')

    racelist1[1] = ab
    racelist1[2] = age
    racelist1[3] = racelist1[3].str.replace('kg', '')

    racelist2 = racelist.iloc[:, -1:]
    racelist1['team'] = team
    racelist1['origin'] = origin

    racelist = pd.concat([racelist1, racelist2], axis=1)
    racelist.columns = ['player_id', 'AB_class', 'age', 'weight', 'team', 'origin', 'run_once']
    racelist = racelist[~racelist.duplicated()]
    racelist['player_id'] = pd.to_numeric(racelist['player_id'], errors="coerce").astype("Int64")
    racelist.reset_index(drop=True, inplace=True)
    racelist['run_once'] = racelist['run_once'].fillna(1)
    racelist['run_once'] = racelist['run_once'].apply(lambda x: pd.to_numeric(x, errors='coerce')).fillna(0)

    flst_src = tables[1].droplevel(0, axis=1).droplevel(0, axis=1)
    flst = flst_src.iloc[:, 3:4]
    flst = flst['F数 L数 平均ST'].str.split(' ', expand=True).drop([1, 3], axis=1)
    flst[0] = flst[0].str.replace('F', '')
    flst[2] = flst[2].str.replace('L', '')
    flst.columns = ['F', 'L', 'ST_mean']
    flst = flst[::4]
    flst['ST_mean'] = flst['ST_mean'].str.replace('-', '0.19')
    flst.reset_index(drop=True, inplace=True)

    motor_src = tables[1].droplevel(2, axis=1).droplevel(1, axis=1)
    motor = motor_src.iloc[:, :9]

    rate1 = motor['全国'].str.split(' ', expand=True).drop([1, 3], axis=1)
    rate1.columns = ['N_winning_rate', 'N_2rentai_rate', 'N_3rentai_rate']
    rate1 = rate1[~rate1.duplicated()]

    rate2 = motor['当地'].str.split(' ', expand=True).drop([1, 3], axis=1)
    rate2.columns = ['LC_winning_rate', 'LC_2rentai_rate', 'LC_3rentai_rate']
    rate2 = rate2[~rate2.duplicated()]

    rate3 = motor['モーター'].str.split(' ', expand=True).drop([1, 3], axis=1)
    rate3.columns = ['motor_number', 'motor_2rentai_rate', 'motor_3rentai_rate']
    rate3 = rate3[~rate3.duplicated()]

    rate4 = motor['ボート'].str.split(' ', expand=True).drop([1, 3], axis=1)
    rate4.columns = ['boat_number', 'boat_2rentai_rate', 'boat_3rentai_rate']
    rate4 = rate4[~rate4.duplicated()]

    motor = pd.concat([rate1, rate2, rate3, rate4], axis=1)
    motor.reset_index(drop=True, inplace=True)

    raw = pd.concat([racelist, flst, motor], axis=1)

    # pcexpect（予想印／レース名・条件）
    soup_px = get_soup("pcexpect", online, date, jcd, rno)
    pred_mark = ['0'] * 6
    race_name = ''
    pre1 = ''
    pre2 = ''
    try:
        if soup_px is not None:
            selector_1 = 'body > main > div > div > div > div.contentsFrame1_inner > div:nth-child(6)'
            selector_2 = 'tr:nth-child(1) > td:nth-child(1)'
            mark_table = soup_px.select_one(selector_1)
            mark_cell = mark_table.select(selector_2) if mark_table else []
            tmp = []
            for mark in mark_cell:
                if re.search(r"icon_mark1_", str(mark)):
                    tmp.append(str(mark)[81:82])
                else:
                    tmp.append("0")
            conversion = {'1': '4', '2': '3', '3': '2', '4': '1', '0': '0'}
            pred_mark = [conversion.get(v, '0') for v in tmp] if tmp else pred_mark

            # レース名と条件
            try:
                text_race2 = soup_px.select_one('.title16__add2020').text
                re_race2 = re.findall(r'\w+', text_race2)
                if re_race2[-1] in ('1800m', '1200m'):
                    race_name = re_race2[-2]; pre1=''; pre2=''
                elif re_race2[-1] == '進入固定':
                    race_name = re_race2[-3]; pre1='進入固定'; pre2=''
                elif re_race2[-1] == '安定板使用':
                    if re_race2[-2] == '進入固定':
                        race_name = re_race2[-4]; pre1='進入固定'; pre2='安定板使用'
                    else:
                        race_name = re_race2[-3]; pre1=''; pre2='安定板使用'
            except Exception:
                pass
    except Exception:
        pass

    raw['pred_mark'] = pred_mark
    raw['race_name'] = race_name
    raw['precondition_1'] = pre1
    raw['precondition_2'] = pre2

    # timetable（時刻表：racelist 先頭テーブル）
    try:
        timetable_tables = read_html_tables_robust(str(soup_rl) if soup_rl is not None else racelist_url)
        # FutureWarning 回避: 明示的に iloc[row, col]
        end_time = timetable_tables[0].iloc[0, int(rno) + 1] if len(timetable_tables) > 0 else ""
    except Exception:
        end_time = ""
    race_id = f"{date}{jcd}{str(rno).zfill(2)}"
    raw['race_id'] = race_id
    raw['date'] = date
    raw['code'] = jcd
    raw['R'] = int(rno)
    raw['timetable'] = end_time

    # beforeinfo（展示/気象）
    soup_bf = get_soup("beforeinfo", online, date, jcd, rno)
    if soup_bf is not None:
        try:
            df_b = read_html_tables_robust(str(soup_bf))
        except Exception:
            df_b = []
    else:
        df_b = []

    if df_b:
        beforeinfo = df_b[1]
        beforeinfo = beforeinfo.droplevel(0, axis=1)
        beforeinfo = beforeinfo.iloc[:, 3:8]
        beforeinfo = beforeinfo[3::4].reset_index(drop=True)

        ex_entry = df_b[2]
        try:
            ex_entry = ex_entry.droplevel(0, axis=1)
            ex_entry['entry'] = ex_entry['コース'].str[:1]
            ex_entry['ST_tenji'] = ex_entry['コース'].str[-4:]
        except Exception:
            ex_entry = pd.DataFrame({"entry": list("123456"), "ST_tenji": [np.nan]*6})

        # ST_tenji を "0.XX" 形式に正規化（最終的には parse_st が面倒を見る）
        for idx_, row_ in ex_entry.iterrows():
            if pd.notnull(row_['ST_tenji']):
                row_['ST_tenji'] = '0' + str(row_['ST_tenji']).strip()

        # --- 展示（コース / ST_tenji）: wakuban順で横結合する ---
        # 1) 枠番順の選手（df_b[1]）
        ex_member_src = df_b[1][['枠', 'ボートレーサー']].droplevel(0, axis=1)
        ex_member = (
            ex_member_src
              .assign(wakuban=lambda d: pd.to_numeric(d['枠'], errors='coerce').astype('Int64'))
              .dropna(subset=['wakuban'])
              .drop_duplicates(subset=['wakuban'])
              .sort_values('wakuban')
              .rename(columns={'ボートレーサー': 'player'})[['player', 'wakuban']]
              .reset_index(drop=True)
        )
        
        # 2) コース表（df_b[2]）から “展示コース” と “展示ST”
        ex2 = df_b[2].droplevel(0, axis=1).copy()
        ex2 = ex2[ex2['コース'].notna()].copy()
        ex2 = ex2.head(6).reset_index(drop=True)
        
        # 展示コース（先頭の1桁数字を拾う → 1..6 以外はNAに）
        ex2['entry_tenji'] = (
            ex2['コース'].astype(str).str.extract(r'(\d)', expand=False)
               .apply(lambda s: pd.to_numeric(s, errors='coerce')).astype('Int64')
        )
        
        # 展示ST（後段で parse_st が面倒を見るので生のまま抽出）
        st_pat = r'([FL]?\.\d{2}|[FL]\d{2}|\.\d{2}|\d{2})'
        ex2['ST_tenji'] = ex2['コース'].astype(str).str.extract(st_pat, expand=False)
        
        # 3) wakuban順の行に展示情報を横結合（キー結合しない）
        ex_join = pd.concat([ex_member, ex2[['entry_tenji', 'ST_tenji']]], axis=1)

        # --- ここから置換（A=ex2['entry_tenji'], B=1..6 を結合→A昇順→Bを entry_tenji に） ---  # <<< 置換開始アンカー
        # A: コース順に見た「枠番」例: [1,5,2,3,4,6]
        A = ex2['entry_tenji'].astype('Int64').tolist()
        # B: コース番号 1..6
        B = list(range(1, len(A) + 1))  # ふつう6

        # A/B を結合して A で昇順ソート
        map_df = pd.DataFrame({'A_wakuban': A, 'B_course': B})
        map_df_sorted = map_df.sort_values('A_wakuban').reset_index(drop=True)

        # wakuban=1..6（= ex_member の行順）に対応する「真のコース番号」配列
        entry_seq = map_df_sorted['B_course'].astype('Int64').tolist()

        # そのまま entry_tenji に代入（ex_member は枠番昇順で6行固定）
        ex_join['entry_tenji'] = pd.Series(entry_seq, dtype='Int64').values

        # ST は「コース」に紐づくので、B（course）→ST の順で並べ替えて貼る
        st_by_course = dict(zip(B, ex2['ST_tenji']))
        ex_join['ST_tenji'] = [st_by_course[int(c)] if pd.notna(c) else np.nan for c in ex_join['entry_tenji']]

        # 4) 従来と同じ列で raw 側に渡す
        ex_entry = ex_join[['player', 'wakuban', 'entry_tenji', 'ST_tenji']].copy().reset_index(drop=True)
        # --- 置換ここまで ---  # <<< 置換終了アンカー



        # 気象
        soup_b = BeautifulSoup(str(soup_bf), 'html.parser')
        weather_info = soup_b.find('p', class_='weather1_title')
        observation_time = weather_info.text.strip() if weather_info else 'N/A'

        def _pick_value(div_cls: str) -> str:
            box = soup_b.find("div", {"class": div_cls})
            if box is None:
                return ""
            span = box.find("span", {"class": "weather1_bodyUnitLabelData"})
            return span.text.strip() if span else ""

        if re.match(r'水面気象情報\s0:00現在', observation_time or ""):
            weather = soup_b.find("div", {"class": "weather1_bodyUnit is-weather"}).find("span", {"class": "weather1_bodyUnitLabelTitle"}).text.strip()
            temperature_value = wind_speed_value = water_temperature_value = wave_height_value = ""
            wind_direction = ""
        else:
            try:
                weather = soup_b.find("div", {"class": "weather1_bodyUnit is-weather"}).find("span", {"class": "weather1_bodyUnitLabelTitle"}).text.strip()
            except Exception:
                weather = ""
            temperature = _pick_value("weather1_bodyUnitLabel")
            wind_speed = _pick_value("weather1_bodyUnit is-wind")
            water_temperature = _pick_value("weather1_bodyUnit is-waterTemperature")
            wave_height = _pick_value("weather1_bodyUnit is-wave")
            try:
                wind_direction_class = soup_b.find("div", {"class": "weather1_bodyUnit is-windDirection"}).find("p")["class"]
                wind_direction = [cls for cls in wind_direction_class if "is-wind" in cls][0].replace("is-", "")
            except Exception:
                wind_direction = ""
            def _num(x, suf):
                try:
                    return float(x.replace(suf, ""))
                except Exception:
                    return np.nan
            temperature_value = _num(temperature, "℃")
            wind_speed_value = _num(wind_speed, "m")
            water_temperature_value = _num(water_temperature, "℃")
            wave_height_value = _num(wave_height, "cm")

        weather_df = pd.DataFrame({
            "temperature": [temperature_value]*6,
            "weather": [weather]*6,
            "wind_speed": [wind_speed_value]*6,
            "wind_direction": [wind_direction]*6,
            "water_temperature": [water_temperature_value]*6,
            "wave_height": [wave_height_value]*6,
        })

        raw = pd.concat([raw, ex_entry, beforeinfo, weather_df], axis=1)

    else:
        # beforeinfo 不在 → 空欄で型を合わせる（wakuban は 1..6 で補完）
        raw = pd.concat([
            raw,
            pd.DataFrame({
                "player": [""]*6,
                "wakuban": list(range(1,7)),
                "entry_tenji": list(range(1,7)),
                "ST_tenji": [np.nan]*6
            }),
            pd.DataFrame(np.nan, index=range(6), columns=[0,1,2,3,4]).rename(
                columns={0:"展示 タイム",1:"チルト",2:"プロペラ",3:"部品交換",4:"調整重量"}),
            pd.DataFrame({
                "temperature": [np.nan]*6, "weather": ["" ]*6, "wind_speed": [np.nan]*6,
                "wind_direction": ["" ]*6, "water_temperature":[np.nan]*6, "wave_height":[np.nan]*6,
            })
        ], axis=1)

    # raceindex（性別）
    soup_ri = get_soup("raceindex", online, date, jcd)
    if soup_ri is not None:
        tds = soup_ri.find_all("td")
        names, genders = [], []
        for td in tds:
            a_tag = td.find("a")
            if a_tag and "/owpc/pc/data/racersearch/profile" in a_tag.get("href", ""):
                name = a_tag.text.strip().replace("　　　　", " ").replace("　　　", " ").replace("　　", " ").replace("　", " ").strip()
                gender = "不明"
                for div in td.find_all("div"):
                    cls = div.get("class", [])
                    if "is-lady" in cls:
                        gender = "女性"; break
                    if "is-empty" in cls:
                        gender = "男性"; break
                names.append(name)
                genders.append(gender)
        players_genders = pd.DataFrame({"player": names, "sex": genders}).drop_duplicates()
        raw = pd.merge(raw, players_genders, on='player', how='left')

    # pay/index 情報を code で付与
    raw = pd.merge(raw, race_merged, on="code", how="left")

    # 列名置換（学習ノート準拠）
    column_name_map = {
        "調整重量": "counter_weight",
        "展示 タイム": "time_tenji",
        "チルト": "Tilt",
        "プロペラ": "propeller",
        "部品交換": "parts_exchange",
    }
    raw.rename(columns=column_name_map, inplace=True)

    # 将来のラグ特徴用に、空列の entry / is_wakunari を用意（Int64, NA）
    for col in ("entry", "is_wakunari"):
        if col not in raw.columns:
            raw[col] = pd.Series([pd.NA] * len(raw), dtype="Int64")
        else:
            try:
                raw[col] = pd.to_numeric(raw[col], errors="coerce").astype("Int64")
            except Exception:
                raw[col] = pd.Series([pd.NA] * len(raw), dtype="Int64")

    # ライブでは得られない“結果系”のダミー列（必要に応じて追加/削除）
    for missing_col in ["ST", "ST_rank", "rank", "winning_trick", "henkan_ticket", "remarks"]:
        if missing_col not in raw.columns:
            raw[missing_col] = np.nan if missing_col not in ("winning_trick","henkan_ticket","remarks") else ""

    # 節ID（schedule開始日優先）
    # - 日次raw（build_raw_csv.py）と同一仕様に揃える
    # - schedule開始日が取れない場合のみ date にフォールバック
    raw["section_id"], fallback = compute_section_id_from_schedule(raw)
    fallback_n = int(pd.Series(fallback).sum())
    if fallback_n:
        print(f"[WARN] section_id: schedule開始日が取れず date にフォールバックした行数={fallback_n}")

    # 型の軽い正規化（数値っぽいものを数値化）
    numeric_guess_cols = ["age","weight","F","L","ST_mean","N_winning_rate","N_2rentai_rate","N_3rentai_rate",
                          "LC_winning_rate","LC_2rentai_rate","LC_3rentai_rate",
                          "motor_number","motor_2rentai_rate","motor_3rentai_rate",
                          "boat_number","boat_2rentai_rate","boat_3rentai_rate",
                          "entry_tenji","wind_speed","water_temperature","wave_height","counter_weight","wakuban"]
    for c in numeric_guess_cols:
        if c in raw.columns:
            raw[c] = pd.to_numeric(raw[c], errors="coerce")

    return raw

# =============== 列順アライン（自動: data/raw の最新 *_raw.csv） ===============
def find_latest_raw_csv(raw_dir: str) -> Optional[str]:
    p = Path(raw_dir)
    if not p.exists():
        return None
    files = sorted(
        p.glob("*_raw.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True
    )
    return str(files[0]) if files else None

def align_columns(df: pd.DataFrame, reference_csv: str) -> pd.DataFrame:
    """参照CSVの列順に合わせ、足りない列はNA追加、余分は削除して返す"""
    ref = pd.read_csv(reference_csv, nrows=1)
    ref_cols: List[str] = list(ref.columns)
    # 追加（欠損で埋める）
    for c in ref_cols:
        if c not in df.columns:
            df[c] = pd.NA
    # 並べ替え＆余分を落とす
    df = df[ref_cols]
    return df

# =============== メイン ===============
def main():
    ap = argparse.ArgumentParser(description="Build one-race live raw CSV (no raceresult)")
    ap.add_argument("--date", required=True, help="YYYYMMDD")
    ap.add_argument("--jcd",  required=True, help="場コード 2桁 or 数値（例: 12）")
    ap.add_argument("--race", required=True, help="レース番号 1-12")
    ap.add_argument("--online", action="store_true", help="公式サイトから直接取得（live/html にキャッシュ）")
    ap.add_argument("--out", help="出力CSV（省略時 data/live/raw_YYYYMMDD_JCD_R.csv）")
    args = ap.parse_args()

    date = normalize_yyyymmdd(args.date)
    jcd = str(args.jcd).zfill(2)
    rno = str(int(args.race))

    out_csv = args.out or data_path("live", f"raw_{date}_{jcd}_{rno}.csv")
    ensure_dir(out_csv)

    try:
        df = build_live_raw(date, jcd, rno, args.online)
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)

    # 6行になっているか保険
    if len(df) != 6:
        print(f"[WARN] rows != 6 (got {len(df)}). 継続します。", file=sys.stderr)

    # ▼ デフォルトで data/raw の最新 *_raw.csv に自動整列
    latest_ref = find_latest_raw_csv(data_path("raw"))
    if latest_ref:
        try:
            df = align_columns(df, latest_ref)
            print(f"[INFO] 列順を最新の参照CSVに合わせました: {latest_ref}")
        except Exception as e:
            print(f"[WARN] 列順アラインに失敗: {type(e).__name__}: {e}", file=sys.stderr)
    else:
        print(f"[WARN] 参照CSVが見つかりませんでした（data/raw/*_raw.csv）。列順は現状のまま保存します。", file=sys.stderr)

    # === ここから追加：前処理器（v1.0.2）に合わせて ST_tenji → 数値化、ST_tenji_rank を生成 ===
    # ST_tenji が無ければ作っておく（NaN）
    if "ST_tenji" not in df.columns:
        df["ST_tenji"] = np.nan
    # 数値化（符号付き秒：F→負、L→正）
    df["ST_tenji"] = df["ST_tenji"].apply(parse_st).astype(float)

    # レース内順位（小さい=早い → 1位）※ 単一レースでも race_id で groupby してOK
    if "race_id" in df.columns:
        df["ST_tenji_rank"] = (
            df.groupby("race_id")["ST_tenji"]
              .rank(method="min", ascending=True)
              .astype("Int64")
        )
    else:
        df["ST_tenji_rank"] = df["ST_tenji"].rank(method="min", ascending=True).astype("Int64")

    # ガード：展示STが非数値（L 単独等）を含むレースは推論中止（現行ルール）
    if df["ST_tenji"].isna().any():
        print("[ERROR] 推論中止: 展示STに非数値（L等）が含まれています。", file=sys.stderr)
        sys.exit(2)
    # === 追加ここまで ===

    df.to_csv(out_csv, index=False, encoding="utf_8_sig")
    print(f"[OK] saved: {out_csv}  (rows={len(df)}, cols={len(df.columns)})")

    # 1行プレビュー
    with pd.option_context("display.max_columns", None):
        print(df.head(1).to_string(index=False))


if __name__ == "__main__":
    main()
