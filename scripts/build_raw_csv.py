# scripts/build_raw_csv.py
import os
import re
import glob
import argparse
import traceback
from datetime import datetime

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm


# =============== ユーティリティ ===============

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def parse_args():
    p = argparse.ArgumentParser(description="Boatrace bin(HTML) → 日次 raw/refund CSV 生成")
    p.add_argument(
        "--date",
        help="対象日（YYYY-MM-DD または YYYYMMDD）。省略時は今日。",
        default=None,
    )
    return p.parse_args()

def normalize_yyyymmdd(s: str | None) -> str:
    if s is None:
        return datetime.now().strftime("%Y%m%d")
    s = s.strip()
    if re.fullmatch(r"\d{8}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y%m%d")
    raise ValueError("Invalid --date format. Use YYYY-MM-DD or YYYYMMDD.")

def load_html(path: str) -> BeautifulSoup | None:
    try:
        with open(path, "rb") as f:
            html = f.read().decode("utf-8", errors="ignore")
        return BeautifulSoup(html, "html.parser")
    except Exception:
        print(f"[ERROR] failed to load {path}")
        traceback.print_exc()
        return None


# =============== メイン ===============

def main():
    args = parse_args()
    date = normalize_yyyymmdd(args.date)  # YYYYMMDD
    print(f"処理する日付: {date}")

    # 出力先フォルダ
    ensure_dir("data/raw")
    ensure_dir("data/refund")

    # ---------- pay（開催一覧） ----------
    pay_path = f"data/html/pay/pay{date}.bin"
    soup = load_html(pay_path)
    if soup is None:
        print(f"[ERROR] pay ファイルがありません: {pay_path}")
        return

    race_tracks = []
    image_tags = soup.find_all('img', alt=True)
    for img in image_tags:
        alt_text = img['alt']
        # 日本語を含む ALT のみ対象
        if re.search(r'[\u3000-\u303F\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', alt_text):
            src = img.get('src', '')
            m = re.search(r'text_place1_(\d+)\.png', src)
            if m:
                track_code = m.group(1)
                race_tracks.append((alt_text, track_code))

    # pay → 会場名・コード・グレード/属性など
    race_data = []
    for venue_name, track_code in race_tracks:
        area_name_tag = soup.find('img', alt=lambda x: x and venue_name.strip() in x.strip())
        if not area_name_tag:
            continue
        parent_div = area_name_tag.find_parent('div', class_='table1_area')

        # レースグレード
        race_grade = "不明"
        area_type_tag = parent_div.find('p', class_='table1_areaType')
        if area_type_tag:
            for cls in area_type_tag.get('class', []):
                if cls.startswith('is-'):
                    race_grade = cls.replace("is-", "")
                    break

        # レースタイプ
        race_type = "分類なし"
        area_time_tag = parent_div.find('p', class_='table1_areaTime')
        if area_time_tag:
            for cls in area_time_tag.get('class', []):
                if cls.startswith('is-'):
                    race_type = cls.replace("is-", "")
                    break

        # レース属性
        race_attribute = "分類なし"
        area_women_tag = parent_div.find('p', class_='table1_areaWomen')
        if area_women_tag:
            for cls in area_women_tag.get('class', []):
                if cls.startswith('is-'):
                    race_attribute = cls.replace("is-", "")
                    break

        race_data.append((venue_name, track_code, race_grade, race_type, race_attribute))

    df_pay = pd.DataFrame(race_data, columns=['place', 'code', 'race_grade', 'race_type', 'race_attribute'])

    # ---------- index（開催タイトル/日次/会期） ----------
    index_path = f"data/html/index/index{date}.bin"
    soup = load_html(index_path)
    if soup is None:
        print(f"[ERROR] index ファイルがありません: {index_path}")
        return

    data_idx = []
    rows = soup.find_all("td", class_="is-arrow1")
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

        # レースグレード/属性（is-xxx クラス）
        grade_td = row.find_next("td", class_=lambda x: x and x.startswith("is-") and x != "is-p10-10")
        if grade_td:
            race_classes = [cls for cls in grade_td.get("class", []) if cls.startswith("is-")]
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

        # 会期日数の計算
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
            except Exception as e:
                print(f"Error calculating duration for {schedule}: {e}")

        # 「初日/最終日/○日目」を数値化
        day_number = None
        if day and schedule:
            try:
                if "最終日" in day:
                    day_number = duration
                elif "初日" in day:
                    day_number = 1
                else:
                    day_number = int(''.join(filter(str.isdigit, day)))
            except Exception as e:
                print(f"Error calculating day number for {day}: {e}")

        if venue_name:
            data_idx.append({
                "place": venue_name,
                "title": title,
                "schedule": schedule,
                "day": day_number,
                "race_grade_idx": race_grade,
                "race_attribute_idx": race_attribute,
                "section": duration
            })

    df_index = pd.DataFrame(data_idx)
    df_index_subset = df_index[["place", "title", "day", "section", "schedule"]]

    # 住之江の二重（電話投票テーブル）など重複があれば末尾を落とす
    if df_index_subset['place'].duplicated().any():
        df_index_subset = df_index_subset[:-1]

    # place で pay と index を結合 → 会場コード付きの開催情報
    race_merged = pd.merge(df_pay, df_index_subset, on="place", how="left")
    code_list = race_merged['code'].tolist()
    print(f'【{date}】の開催は【{len(code_list)}】場です')

    # ---------- 各会場 × 12R ----------
    for code in code_list:
        for rno in tqdm(range(1, 13)):
            try:
                race_id = f"{date}{code}{str(rno).zfill(2)}"

                # ===== racelist =====
                with open(f'data/html/racelist/racelist{race_id}.bin', 'rb') as file:
                    racelist_content = file.read()
                df = pd.read_html(racelist_content)

                try:
                    racelist = df[1]
                    racelist = racelist.droplevel(0, axis=1).droplevel(0, axis=1)
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
                    racelist_columns = ['player_id', 'AB_class', 'age', 'weight', 'team', 'origin', 'run_once']
                    racelist.columns = racelist_columns
                    racelist = racelist[~racelist.duplicated()]
                    racelist['player_id'] = racelist['player_id'].astype(int)
                    racelist.reset_index(inplace=True, drop=True)
                    racelist['run_once'] = racelist['run_once'].fillna(1)
                    racelist['run_once'] = racelist['run_once'].apply(lambda x: pd.to_numeric(x, errors='coerce')).fillna(0)

                    flst = df[1].droplevel(0, axis=1).droplevel(0, axis=1)
                    flst = flst.iloc[:, 3:4]
                    flst = flst['F数 L数 平均ST'].str.split(' ', expand=True).drop([1, 3], axis=1)
                    flst[0] = flst[0].str.replace('F', '')
                    flst[2] = flst[2].str.replace('L', '')
                    flst.columns = ['F', 'L', 'ST_mean']
                    flst = flst[::4]
                    flst['ST_mean'] = flst['ST_mean'].str.replace('-', '0.19')
                    flst.reset_index(inplace=True, drop=True)

                    motor = df[1]
                    motor = motor.droplevel(2, axis=1).droplevel(1, axis=1)
                    motor = motor.iloc[:, :9]

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
                    motor.reset_index(inplace=True, drop=True)

                    raw = pd.concat([racelist, flst, motor], axis=1)

                    # ===== pcexpect =====
                    with open(f'data/html/pcexpect/pcexpect{race_id}.bin', 'rb') as file:
                        pcexpect_content = file.read()
                    soup_px = BeautifulSoup(pcexpect_content, 'html.parser')

                    selector_1 = 'body > main > div > div > div > div.contentsFrame1_inner > div:nth-child(6)'
                    selector_2 = 'tr:nth-child(1) > td:nth-child(1)'
                    try:
                        mark_table = soup_px.select_one(selector_1)
                        mark_cell = mark_table.select(selector_2)
                        mark_list = []
                        for mark in mark_cell:
                            if re.search(r"icon_mark1_", str(mark)):
                                mark_list.append(str(mark)[81:82])
                            else:
                                mark_list.append("0")
                        conversion = {'1': '4', '2': '3', '3': '2', '4': '1', '0': '0'}
                        mark_list = [conversion[v] for v in mark_list]
                        raw['pred_mark'] = mark_list
                    except Exception:
                        raw['pred_mark'] = ['0'] * 6

                    # レース名と条件
                    try:
                        text_race2 = soup_px.select_one('.title16__add2020').text
                        re_race2 = re.findall(r'\w+', text_race2)
                        if re_race2[-1] in ('1800m', '1200m'):
                            raw['race_name'] = re_race2[-2]
                            raw['precondition_1'] = ''
                            raw['precondition_2'] = ''
                        elif re_race2[-1] == '進入固定':
                            raw['race_name'] = re_race2[-3]
                            raw['precondition_1'] = re_race2[-1]
                            raw['precondition_2'] = ''
                        elif re_race2[-1] == '安定板使用':
                            if re_race2[-2] == '進入固定':
                                raw['race_name'] = re_race2[-4]
                                raw['precondition_1'] = re_race2[-2]
                                raw['precondition_2'] = re_race2[-1]
                            else:
                                raw['race_name'] = re_race2[-3]
                                raw['precondition_1'] = ''
                                raw['precondition_2'] = re_race2[-1]
                    except Exception:
                        raw['race_name'] = ''
                        raw['precondition_1'] = ''
                        raw['precondition_2'] = ''

                    # 発走時刻（時刻表）
                    end_time = df[0].iloc[0][int(rno) + 1]
                    raw['race_id'] = race_id
                    raw['date'] = date
                    raw['code'] = code
                    raw['R'] = int(rno)
                    raw['timetable'] = end_time

                    # ===== beforeinfo =====
                    with open(f'data/html/beforeinfo/beforeinfo{race_id}.bin', 'rb') as file:
                        beforeinfo_content = file.read()
                    df_b = pd.read_html(beforeinfo_content)

                    beforeinfo = df_b[1]
                    beforeinfo = beforeinfo.droplevel(0, axis=1)
                    beforeinfo = beforeinfo.iloc[:, 3:8]
                    beforeinfo = beforeinfo[3::4]
                    beforeinfo.reset_index(inplace=True, drop=True)

                    ex_entry = df_b[2]
                    try:
                        ex_entry = ex_entry.droplevel(0, axis=1)
                        ex_entry['entry'] = ex_entry['コース'].str[:1]
                        ex_entry['ST_tenji'] = ex_entry['コース'].str[-4:]
                    except Exception:
                        print(f'【{race_id}】スタート展示がないようです')
                        ex_entry['entry'] = ['1', '2', '3', '4', '5', '6']
                        ex_entry['ST_tenji'] = np.nan

                    for idx_, row_ in ex_entry.iterrows():
                        if pd.notnull(row_['ST_tenji']):
                            row_['ST_tenji'] = '0' + row_['ST_tenji'].strip()

                    ex_member = df_b[1][['枠', 'ボートレーサー']].droplevel(0, axis=1)
                    ex_member = ex_member[~ex_member['枠'].astype(int).duplicated()]
                    ex_member.columns = ['entry', 'player']
                    ex_member['entry'] = ex_member['entry'].astype(str)

                    num = []
                    ex_entry_member = pd.merge(ex_entry, ex_member, on='entry', how='left').drop(['コース', '並び', 'ST'], axis=1, errors='ignore')
                    ex_member.rename(columns={'entry': '枠'}, inplace=True)
                    [num.append(e + 1) for e in range(len(df_b[2]))]
                    ex_entry_member['entry'] = num
                    ex_entry = pd.merge(ex_member, ex_entry_member, on='player', how='left')
                    ex_entry.rename(columns={'entry': 'entry_tenji'}, inplace=True)
                    ex_entry.drop(['player', '枠'], axis=1, inplace=True)
                    ex_entry.reset_index(inplace=True, drop=True)

                    # 気象
                    soup_b = BeautifulSoup(beforeinfo_content, 'html.parser')
                    weather_info = soup_b.find('p', class_='weather1_title')
                    observation_time = weather_info.text.strip() if weather_info else 'N/A'

                    if re.match(r'水面気象情報\s0:00現在', observation_time):
                        weather = soup_b.find("div", {"class": "weather1_bodyUnit is-weather"}).find("span", {"class": "weather1_bodyUnitLabelTitle"}).text.strip()
                        temperature_value = 'N/A'
                        wind_speed_value = 'N/A'
                        water_temperature_value = 'N/A'
                        wave_height_value = 'N/A'
                        wind_direction = 'N/A'
                    else:
                        temperature = soup_b.find("div", {"class": "weather1_bodyUnitLabel"}).find("span", {"class": "weather1_bodyUnitLabelData"}).text.strip()
                        weather = soup_b.find("div", {"class": "weather1_bodyUnit is-weather"}).find("span", {"class": "weather1_bodyUnitLabelTitle"}).text.strip()
                        wind_speed = soup_b.find("div", {"class": "weather1_bodyUnit is-wind"}).find("span", {"class": "weather1_bodyUnitLabelData"}).text.strip()
                        water_temperature = soup_b.find("div", {"class": "weather1_bodyUnit is-waterTemperature"}).find("span", {"class": "weather1_bodyUnitLabelData"}).text.strip()
                        wave_height = soup_b.find("div", {"class": "weather1_bodyUnit is-wave"}).find("span", {"class": "weather1_bodyUnitLabelData"}).text.strip()

                        wind_direction_class = soup_b.find("div", {"class": "weather1_bodyUnit is-windDirection"}).find("p")["class"]
                        wind_direction = [cls for cls in wind_direction_class if "is-wind" in cls][0].replace("is-", "")

                        temperature_value = float(temperature.replace("℃", ""))
                        wind_speed_value = float(wind_speed.replace("m", ""))
                        water_temperature_value = float(water_temperature.replace("℃", ""))
                        wave_height_value = float(wave_height.replace("cm", ""))

                    weather_df = pd.DataFrame({
                        "temperature": [temperature_value],
                        "weather": [weather],
                        "wind_speed": [wind_speed_value],
                        "wind_direction": [wind_direction],
                        "water_temperature": [water_temperature_value],
                        "wave_height": [wave_height_value],
                    })
                    weather_df = pd.concat([weather_df] * 6).reset_index(drop=True)

                    raw = pd.concat([raw, ex_entry, beforeinfo, weather_df], axis=1)

                    # ===== raceresult =====
                    with open(f'data/html/raceresult/raceresult{race_id}.bin', 'rb') as file:
                        raceresult_content = file.read()
                    df_r = pd.read_html(raceresult_content)

                    num = []
                    df_r[1]['player_id'] = df_r[1]['ボートレーサー'].str[:4]
                    df_r[1]['player'] = df_r[1]['ボートレーサー'].str[4:]
                    df_r[1]['player'] = (df_r[1]['player'].str.replace("　　　", " ")
                                         .str.replace("　　", " ")
                                         .str.replace("　", " ")
                                         .str.strip())
                    df_r[1] = df_r[1].drop(['ボートレーサー', 'レースタイム'], axis=1, errors='ignore')
                    df_r[1] = df_r[1].sort_values('枠').reset_index(drop=True)

                    df_r[2]['entry'] = df_r[2]['スタート情報'].str[:1].astype(int)
                    df_r[2]['ST'] = df_r[2]['スタート情報'].str[3:7].map(lambda x: '0' + x.strip())
                    df_r[2]['ST'] = pd.to_numeric(df_r[2]['ST'], errors='coerce')
                    entry = df_r[2].drop('スタート情報', axis=1)
                    entry['ST_rank'] = entry['ST'].rank(method='min', ascending=True)

                    member = df_r[1].loc[:, ['player', '枠']]
                    member.columns = ['player', 'entry']
                    entry_member = pd.merge(entry, member, on='entry', how='left')
                    [num.append(e + 1) for e in range(len(entry))]
                    entry_member['entry'] = num
                    finish = pd.merge(df_r[1], entry_member, on='player', how='left')
                    finish.drop('player_id', axis=1, inplace=True)

                    # 備考
                    remarks2 = df_r[5].iloc[0, 0] if len(df_r[5].index) else ''
                    raw['winning_trick'] = remarks2
                    if len(df_r[4].index) != 0:
                        raw['henkan_ticket'] = df_r[4].iloc[0, 0]
                    else:
                        raw['henkan_ticket'] = ''
                    if len(df_r[6].index) != 0:
                        raw['remarks'] = df_r[6].iloc[0, 0].replace('【', '').replace('】', '')
                    else:
                        raw['remarks'] = ''

                    raw = pd.concat([raw, finish], axis=1)

                    # ===== raceindex（選手性別など） =====
                    with open(f'data/html/raceindex/raceindex{date}{code}.bin', 'rb') as file:
                        raceindex_content = file.read()
                    soup_ri = BeautifulSoup(raceindex_content, 'html.parser')

                    players = soup_ri.find_all("td")
                    names, genders = [], []
                    for td in players:
                        a_tag = td.find("a")
                        if a_tag and "/owpc/pc/data/racersearch/profile" in a_tag.get("href", ""):
                            name = a_tag.text.strip().replace("　　　　", " ").replace("　　　", " ").replace("　　", " ").replace("　", " ")
                            names.append(name)
                            gender = "不明"
                            for div in td.find_all("div"):
                                cls = div.get("class", [])
                                if "is-lady" in cls:
                                    gender = "女性"; break
                                elif "is-empty" in cls:
                                    gender = "男性"; break
                            genders.append(gender)

                    players_genders = pd.DataFrame({"player": names, "sex": genders}).drop_duplicates()
                    players_genders["player"] = players_genders["player"].str.strip()

                    raw = pd.merge(raw, players_genders, on='player', how='left')
                    raw = pd.merge(raw, race_merged, on="code", how="left")

                    # 枠なりフラグ
                    raw['entry_tenji'] = raw['entry_tenji'].astype(float)
                    raw['枠'] = raw['枠'].astype(float)

                    def check_wakunari(group: pd.DataFrame) -> pd.DataFrame:
                        group['is_wakunari'] = 1 if (group['entry_tenji'] == group['枠']).all() else 0
                        return group

                    raw = raw.groupby('race_id', as_index=False).apply(check_wakunari)

                    # ===== refund（払戻） =====
                    refund = df_r[3].drop([1, 3, 5, 7, 11, 12, 14, 17], axis=0, errors='ignore').reset_index(drop=True)
                    refund.insert(0, 'race_id', race_id)

                    # 一時保存（後で日全体に結合）
                    raw.to_pickle(f'data/{race_id}_raw.pickle')
                    refund.to_pickle(f'data/{race_id}_refund.pickle')

                except Exception:
                    print(f'【{race_id}】のレース結果はありません！')
                    tb = traceback.format_exc()
                    print("エラー情報\n" + tb)
                    if "remarks2 = df[5].iloc[0,0]" in tb:
                        print(f'【{race_id}】レース不成立')
                    elif "ex_entry['entry'] = ex_entry['コース'].str[:1]" in tb:
                        print(f'【{race_id}】レース中止')
                    elif "df[1]['player_id'] = df[1]['ボートレーサー'].str[:4]" in tb:
                        print(f'【{race_id}】荒天によるレース中止')

            except Exception:
                print(f'【{date}{code}{str(rno).zfill(2)}】アクシデントによるレース中止')

    # ---------- 日の raw/refund を結合して出力 ----------
    # raw
    ensure_dir("data/raw")
    pickle_files = glob.glob(f'data/{date}*_raw.pickle')
    if pickle_files:
        raw_list = [pd.read_pickle(p) for p in pickle_files]
        raw = pd.concat(raw_list, axis=0).reset_index(drop=True)

        # 列名の置換（ノート準拠）
        column_name_map = {
            "調整重量": "counter_weight",
            "展示 タイム": "time_tenji",
            "チルト": "Tilt",
            "プロペラ": "propeller",
            "部品交換": "parts_exchange",
            "着": "rank",
            "枠": "wakuban"
        }
        raw.rename(columns=column_name_map, inplace=True)

        out_raw = f'data/raw/{date}_raw.csv'
        raw.to_csv(out_raw, index=False, encoding="utf_8_sig")
        print(f"保存しました: {out_raw} ({len(raw)} 行, {len(raw.columns)} 列)")
    else:
        print("[WARN] raw の pickle が見つかりませんでした")

    # refund
    ensure_dir("data/refund")
    refund_pickles = glob.glob(f'data/{date}*_refund.pickle')
    if refund_pickles:
        refund_list = [pd.read_pickle(p) for p in refund_pickles]
        refund = pd.concat(refund_list, axis=0).reset_index(drop=True)
        out_refund = f'data/refund/{date}_refund.csv'
        refund.to_csv(out_refund, index=False, encoding="utf_8_sig")
        print(f"保存しました: {out_refund} ({len(refund)} 行, {len(refund.columns)} 列)")
    else:
        print("[WARN] refund の pickle が見つかりませんでした")

    # 一時 pickle を削除
    for file_path in glob.glob('data/*.pickle'):
        try:
            os.remove(file_path)
        except Exception:
            pass

    print("処理が終了しました")


if __name__ == "__main__":
    main()
