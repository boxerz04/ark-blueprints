import os
import csv
import argparse
from bs4 import BeautifulSoup
import glob
import math
import re

def get_series_info(table_header):
    # table_header は <thead> の <tr> または <th> を想定
    series_name = ""
    
    # 1. クラス名から判定 (ヴィーナス、ルーキー等)
    women_tag = table_header.find('p', class_='table1_areaWomen')
    if women_tag:
        class_str = " ".join(women_tag.get('class', []))
        if 'is-venus' in class_str:
            series_name = "ヴィーナスシリーズ"
        elif 'is-lady' in class_str:
            series_name = "オールレディース"
        elif 'is-rookie' in class_str:
            series_name = "ルーキーシリーズ"

    # 2. それでも空なら ippan クラス等を確認
    if not series_name:
        ippan_tag = table_header.find('p', class_='is-ippan')
        if ippan_tag:
            series_name = "一般"
            
    return series_name

def parse_payouts(input_dir, output_csv_path, start_date=None, end_date=None):
    html_files = glob.glob(os.path.join(input_dir, "*.html"))
    if not html_files:
        print(f"No HTML files found in {input_dir}")
        return
        
    print(f"Found {len(html_files)} HTML files. Starting to parse...")
    
    all_results = []
    special_strings_report = []
    
    for html_file_path in html_files:
        filename = os.path.basename(html_file_path)
        stem = os.path.splitext(filename)[0]
        match = re.search(r"(\d{8})$", stem)
        date_str = match.group(1) if match else stem

        if start_date and date_str < start_date:
            continue
        if end_date and date_str > end_date:
            continue
            
        print(f"Parsing {html_file_path} ...")
        
        with open(html_file_path, "r", encoding="utf-8") as f:
            html_content = f.read()
            
        soup = BeautifulSoup(html_content, "html.parser")
        tables = soup.find_all("table", class_="is-strited1 is-wAuto")
        
        for table in tables:
            thead = table.find("thead")
            if not thead:
                continue
                
            th_elements = thead.find_all("th", class_="is-thColor6")
            
            # 各場のメタ情報（場名、グレード、開催タイプ、日数、シリーズ名）を格納するリスト
            venues_info = []
            
            for th in th_elements:
                div = th.find("div", class_="table1_area type1")
                if not div:
                    continue
                
                # 場名
                img = div.select_one("p.table1_areaName img")
                place = img["alt"].strip() if img and img.has_attr("alt") else "不明"
                
                # グレード
                grade_p = div.find("p", class_="table1_areaType")
                grade_classes = grade_p.get("class", []) if grade_p else []
                grade = "一般"
                if "is-SG" in grade_classes: grade = "SG"
                elif "is-G1a" in grade_classes or "is-G1b" in grade_classes: grade = "G1"
                elif "is-G2a" in grade_classes or "is-G2b" in grade_classes: grade = "G2"
                elif "is-G3a" in grade_classes or "is-G3b" in grade_classes: grade = "G3"
                elif "is-ippan" in grade_classes: grade = "一般"
                
                # 開催タイプ (モーニング、ナイター、通常)
                time_p = div.find("p", class_="table1_areaTime")
                time_classes = time_p.get("class", []) if time_p else []
                race_type = "通常"
                if "is-morning" in time_classes: race_type = "モーニング"
                elif "is-nighter" in time_classes: race_type = "ナイター"
                elif "is-summer" in time_classes: race_type = "サマータイム"
                
                # 節の日数 (初日、2日目、最終日など)
                day_p = div.find("p", class_="table1_areaDate")
                day_text = day_p.text.strip() if day_p else ""
                
                series_name = get_series_info(div)
                
                if not series_name and grade in ["G1", "G2", "G3", "SG"]:
                    series_name = grade
                    
                venues_info.append({
                    "place": place,
                    "grade": grade,
                    "type": race_type,
                    "day": day_text,
                    "series": series_name
                })
                    
            # テーブルボディからレースごとのデータを抽出
            tbodies = table.find_all("tbody")
            for tbody in tbodies:
                tr_elements = tbody.find_all("tr")
                for tr in tr_elements:
                    th_race = tr.find("th")
                    if not th_race:
                        continue
                        
                    race_num_str = th_race.text.strip().replace("R", "")
                    try:
                        race_num = int(race_num_str)
                    except ValueError:
                        race_num = race_num_str
                    
                    tds = tr.find_all("td")
                    
                    for i, v_info in enumerate(venues_info):
                        if i * 3 + 2 >= len(tds):
                            break
                            
                        td_kumi = tds[i * 3]
                        td_payout = tds[i * 3 + 1]
                        td_pop = tds[i * 3 + 2]
                        
                        kumi_spans = td_kumi.select(".numberSet1_number")
                        rank1 = rank2 = rank3 = ""
                        if len(kumi_spans) >= 3:
                            rank1 = kumi_spans[0].text.strip()
                            rank2 = kumi_spans[1].text.strip()
                            rank3 = kumi_spans[2].text.strip()
                        else:
                            kumi_string = next((x.strip() for x in td_kumi.stripped_strings if x.strip()), "")
                            rank1 = kumi_string
                            
                        payout_text = next((x.strip() for x in td_payout.stripped_strings if x.strip()), "")
                        pop_text = next((x.strip() for x in td_pop.stripped_strings if x.strip()), "")
                        
                        if not rank1 and not payout_text:
                            continue

                        def check_and_convert(col_name, val):
                            if not val:
                                return ""
                            clean_val = str(val).replace("¥", "").replace("\\", "").replace("￥", "").replace("円", "").replace(",", "").strip()
                            if clean_val.isdigit():
                                num_val = int(clean_val)
                                if col_name == "払戻金" and num_val > 500000:
                                    special_strings_report.append({
                                        "Date": date_str, "Place": v_info["place"], "Race": race_num,
                                        "Column": col_name, "Value": f"異常値除外({val})"
                                    })
                                    return math.nan
                                return num_val
                            else:
                                special_strings_report.append({
                                    "Date": date_str, "Place": v_info["place"], "Race": race_num,
                                    "Column": col_name, "Value": val
                                })
                                return math.nan

                        r1_val = check_and_convert("1着", rank1)
                        r2_val = check_and_convert("2着", rank2)
                        r3_val = check_and_convert("3着", rank3)
                        payout_val = check_and_convert("払戻金", payout_text)
                        pop_val = check_and_convert("人気", pop_text)
                        
                        all_results.append({
                            "日付": date_str,
                            "場名": v_info["place"],
                            "グレード": v_info["grade"],
                            "開催タイプ": v_info["type"],
                            "日数": v_info["day"],
                            "シリーズ名": v_info["series"],
                            "レース番号": race_num,
                            "1着": r1_val,
                            "2着": r2_val,
                            "3着": r3_val,
                            "払戻金": payout_val,
                            "人気": pop_val
                        })

    # 「日付」→「場名」→「レース番号」順にソート
    all_results.sort(key=lambda x: (x["日付"], x["場名"], x["レース番号"] if isinstance(x["レース番号"], int) else 0))

    for row in all_results:
        if isinstance(row["レース番号"], int):
            row["レース番号"] = f"{row['レース番号']}R"

    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    
    with open(output_csv_path, "w", encoding="utf-8-sig", newline="") as f:
        fieldnames = ["日付", "場名", "グレード", "開催タイプ", "日数", "シリーズ名", "レース番号", "1着", "2着", "3着", "払戻金", "人気"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        for row in all_results:
            writer.writerow(row)
            
    print(f"Successfully extracted and sorted {len(all_results)} records.")
    print(f"Saved to {output_csv_path}")

    if special_strings_report:
        print("\n--- 特殊な文字列（数値化できないデータ）の報告 ---")
        for rep in special_strings_report:
            print(f"日付: {rep['Date']}, 場名: {rep['Place']}, レース: {rep['Race']}R, 列: {rep['Column']}, 文字列: '{rep['Value']}'")
        print("--------------------------------------------------")
    else:
        print("\n特殊な文字列は検出されませんでした。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="払戻金HTMLから追加情報(グレード,日数等)を含めて解析します。")
    parser.add_argument("--in-dir", type=str, default="data/html/payouts", help="入力HTMLフォルダのパス")
    parser.add_argument("--out-csv", type=str, default="data/processed/payouts/all_payout_results.csv", help="出力CSVファイルのパス")
    parser.add_argument("--start-date", type=str, default=None, help="解析開始日付 (YYYYMMDD)")
    parser.add_argument("--end-date", type=str, default=None, help="解析終了日付 (YYYYMMDD)")
    
    args = parser.parse_args()
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    input_dir = os.path.join(project_root, args.in_dir) if not os.path.isabs(args.in_dir) else args.in_dir
    output_path = os.path.join(project_root, args.out_csv) if not os.path.isabs(args.out_csv) else args.out_csv
    
    parse_payouts(input_dir, output_path, args.start_date, args.end_date)
