
---

# data_dictionary.md（差し替え）

```markdown
# 📑 Data Dictionary

Ark Blueprints プロジェクトで生成される **基本CSV（64列）** のカラム仕様です。  
（63列の基本情報 + 節ID 1列）

---

## 1. racelist 系（22列）

| カラム名 | 出典 | 説明 |
|----------|------|------|
| player_id | racelist | 登録番号 |
| AB_class  | racelist | 級別（A1, A2, B1, B2 など） |
| age       | racelist | 年齢 |
| weight    | racelist | 体重 |
| team      | racelist | 所属支部 |
| origin    | racelist | 出身地 |
| run_once  | racelist | 当日1走かどうか（1=一走、0=複数走） |
| F         | racelist | フライング回数 |
| L         | racelist | 出遅れ回数 |
| ST_mean   | racelist | 平均スタートタイミング |
| N_winning_rate     | racelist | 全国勝率 |
| N_2rentai_rate     | racelist | 全国2連対率 |
| N_3rentai_rate     | racelist | 全国3連対率 |
| LC_winning_rate    | racelist | 当地勝率 |
| LC_2rentai_rate    | racelist | 当地2連対率 |
| LC_3rentai_rate    | racelist | 当地3連対率 |
| motor_number       | racelist | モーター番号 |
| motor_2rentai_rate | racelist | モーター2連対率 |
| motor_3rentai_rate | racelist | モーター3連対率 |
| boat_number        | racelist | ボート番号 |
| boat_2rentai_rate  | racelist | ボート2連対率 |
| boat_3rentai_rate  | racelist | ボート3連対率 |

---

## 2. pcexpect 系（4列）

| カラム名 | 出典 | 説明 |
|----------|------|------|
| pred_mark | pcexpect | 公式予想印（数値化済み） |
| race_name | pcexpect | レース名 |
| precondition_1 | pcexpect | レース条件①（進入固定など） |
| precondition_2 | pcexpect | レース条件②（安定板使用など） |

---

## 3. beforeinfo 系（12列）

| カラム名 | 出典 | 説明 |
|----------|------|------|
| entry_tenji | beforeinfo | 展示進入順（数値化） |
| ST_tenji    | beforeinfo | 展示スタートタイミング |
| player      | beforeinfo | 選手名（展示情報基準） |
| counter_weight | beforeinfo | 重量調整（kg） |
| time_tenji  | beforeinfo | 展示タイム |
| Tilt        | beforeinfo | チルト角度 |
| propeller   | beforeinfo | プロペラ状況 |
| parts_exchange | beforeinfo | 部品交換有無 |
| temperature | beforeinfo | 気温 |
| weather     | beforeinfo | 天候 |
| wind_speed  | beforeinfo | 風速（m/s） |
| wind_direction | beforeinfo | 風向き |

---

## 4. raceresult 系（8列）

| カラム名 | 出典 | 説明 |
|----------|------|------|
| rank     | raceresult | 着順 |
| wakuban  | raceresult | 枠番 |
| entry    | raceresult | 進入順 |
| ST       | raceresult | スタートタイミング |
| ST_rank  | raceresult | ST順位（算出列） |
| winning_trick | raceresult | 決まり手 |
| henkan_ticket | raceresult | 返還艇情報 |
| remarks  | raceresult | 備考（途中帰郷・失格など） |

---

## 5. raceindex 系（2列）

| カラム名 | 出典 | 説明 |
|----------|------|------|
| sex   | raceindex | 性別（男性/女性） |
| is_lady | raceindex | レディース戦かどうかのフラグ |

---

## 6. pay/index 系（9列）

| カラム名 | 出典 | 説明 |
|----------|------|------|
| place       | pay/index | 開催場名 |
| code        | pay/index | 開催場コード |
| race_grade  | pay/index | グレード（SG, G1, 一般など） |
| race_type   | pay/index | 種別（昼・ナイターなど） |
| race_attribute | pay/index | 属性（女子戦など） |
| title       | pay/index | 開催タイトル |
| day         | pay/index | 何日目（数値化済み） |
| section     | pay/index | 開催日数 |
| schedule    | pay/index | 開催期間（MM/DD–MM/DD） |

---

## 7. 管理カラム（4列）

| カラム名 | 出典 | 説明 |
|----------|------|------|
| race_id   | 管理 | レース一意ID（YYYYMMDD+場コード+R番号） |
| date      | 管理 | 開催日（YYYYMMDD） |
| R         | 管理 | レース番号（1〜12） |
| timetable | 管理 | 発走予定時刻 |

---

## 8. 加工で追加したカラム（3列）

| カラム名 | 出典 | 説明 |
|----------|------|------|
| is_wakunari | 加工 | 進入が枠なりかどうか（1=枠なり、0=非枠なり） |
| ST_rank     | 加工 | 各レースごとのスタートタイム順位 |
| section_id  | 加工 | 節単位のユニークID（YYYYMMDD_場コード） |

---

## ✅ 合計
- racelist: 22列  
- pcexpect: 4列  
- beforeinfo: 12列  
- raceresult: 8列  
- raceindex: 2列  
- pay/index: 9列  
- 管理カラム: 4列  
- 加工: 3列  
**合計: 64列**
