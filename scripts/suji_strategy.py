# scripts/suji_strategy.py
# ーー スジ舟券の簡易戦略（ability対応・top2pair不使用・1-2着重視・逆転prior連続スケール） ーー
# 攻め艇(2〜6)を推定 → 成功/裏スジの型で 1着-2着 を中心に作り、
# 3着は“弱い重み”だけ掛けてランキング用に差をつける（GUI互換のため3連単キーは維持）
#
# ポイント:
# - ability（全国勝率などの地力指標）を渡すと、「右隣同道(逆転)」の判定/重み付けを ability 主体で行う
# - ability が無い場合は base（当該レースの事前確率）でフォールバック
# - 出力は [( 'i-j-k', score, tag ), ...]
# - min 20 本は廃止。攻め艇数 × top_n で制御

from __future__ import annotations
from typing import Dict, List, Tuple, Optional

# 互換のため型は残すが、pairs（top2pair）は使わない
Pair = Tuple[int, int, float]  # (i, j, p_top2set)  ※本版では未使用


# ====== 小ユーティリティ ======
def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

def _normalize_ability(ability: Dict[int, float]) -> Dict[int, float]:
    """
    ability のスケールを自動整形。
    - 既に 0〜1 っぽい（max<=1.5）ならそのまま
    - そうでなければ、レース内の max で割って 0〜1 に寄せる（相対能力）
    """
    vals = [float(v) for v in ability.values() if v is not None]
    if not vals:
        return {k: 0.0 for k in range(1, 7)}
    mx = max(vals)
    if mx <= 1.5:  # 1.0 を超えても誤差程度なら許容
        return {int(k): float(v) for k, v in ability.items()}
    if mx == 0.0:
        return {int(k): 0.0 for k in ability}
    return {int(k): float(v) / mx for k, v in ability.items()}


# ===== 攻め艇スコア（base主体、abilityはシナジーに微反映） =====
def _attacker_scores(base: Dict[int, float], ability: Optional[Dict[int, float]] = None) -> Dict[int, float]:
    """
    攻め艇スコア（2〜6のみ）。
    - 基本は base[k]
    - 1が弱いほど外上積み（b1<0.6）
    - 角(4) +5%、カド受け(3) +2%
    - 右隣シナジー:
        * base の nb/att 比に加えて、ability を渡されたら ability の nb/att 比も +α
        * ただし影響は控えめ（+0〜20% + 追加で +0〜10%）
    """
    b1 = base.get(1, 0.0)
    scores: Dict[int, float] = {}
    abil_norm = _normalize_ability(ability) if ability else None

    for att in range(2, 7):
        nb = att + 1 if att < 6 else 6
        s = base.get(att, 0.0)

        # 1号艇が盤石でないほど外の攻めが通りやすい補正（b1<0.6 で上振れ）
        s *= (1.0 + max(0.0, 0.6 - b1))

        # 角(4)・カド受け(3)の微ボーナス
        if att == 4:
            s *= 1.05
        if att == 3:
            s *= 1.02

        # 右隣シナジー（base）
        batt = max(1e-9, base.get(att, 0.0))
        bnb  = base.get(nb, 0.0)
        r_base = bnb / batt
        s *= (1.0 + 0.2 * _clip(r_base, 0.0, 1.0))  # 0〜+20%

        # 右隣シナジー（abilityがあれば微増幅）
        if abil_norm:
            a_att = max(1e-9, abil_norm.get(att, 0.0))
            a_nb  = abil_norm.get(nb, 0.0)
            r_abil = a_nb / a_att
            s *= (1.0 + 0.10 * _clip(r_abil, 0.0, 1.0))  # 0〜+10%

        scores[att] = s
    return scores


# ===== 候補生成（1-2着重視） =====
def _make_candidates_for(
    att: int,
    base: Dict[int, float],
    ability: Optional[Dict[int, float]] = None,
    event_grade: str = "normal",  # "normal" | "semi" | "final" | "graded"
) -> List[Tuple[str, float, str]]:
    """
    攻め艇 att に対して、成功スジ/裏スジの候補を列挙。
    スコアは 1着・2着の合成でほとんど決まり、3着は“一応の差付け”にだけ使う。
    逆転(右隣同道)は ability 比率を主に判定・重み付けする（無いときだけ base 比率を使用）。
    """
    cand: List[Tuple[str, float, str]] = []
    nb = att + 1 if att < 6 else 6  # 右隣（最外は6に丸め）
    b = base
    abil_norm = _normalize_ability(ability) if ability else None

    # ---- スコア関数（1-2着重視） ----
    # ・core12 = (b[i]^0.85) * (b[j]^0.65) で 1→2 の順に重視
    # ・tail は 3着の微小寄与（0.08*b[k]）。ランキングの同点を避ける程度。
    def score(i: int, j: int, k: int, prior: float) -> float:
        bi = max(1e-9, b.get(i, 0.0))
        bj = max(1e-9, b.get(j, 0.0))
        bk = max(1e-9, b.get(k, 0.0))
        core12 = (bi ** 0.85) * (bj ** 0.65)
        tail   = 0.08 * bk
        return prior * (core12 + tail)

    # 3着候補の集合（ユニーク維持のため i,j は除外）
    def thirds_excluding(i: int, j: int) -> List[int]:
        return [x for x in range(1, 7) if x not in (i, j)]

    # “2着側の基準”を base だけで決める（トップ2ペアは使わない）
    # 1が nb より少しでも強ければ 2着は 1 を優先、そうでなければ nb を優先
    j_pref = 1 if b.get(1, 0.0) >= 0.95 * b.get(nb, 0.0) else nb

    # --- 成功スジ（攻め通る） ---
    # ① 差し成功: att-1-*
    for k in thirds_excluding(att, 1):
        cand.append((f"{att}-{1}-{k}", score(att, 1, k, prior=1.00), "成功/差し"))

    # ② 右隣同道（ツケマイ・絞り成功）: att-nb-*
    if nb != att:
        for k in thirds_excluding(att, nb):
            cand.append((f"{att}-{nb}-{k}", score(att, nb, k, prior=0.95), "成功/右隣同道"))

    # ②' 右隣同道(逆転) : nb-att-*
    #   判定＆priorは ability を主軸（無ければ base でフォールバック）
    b1   = b.get(1, 0.0)
    batt = max(1e-9, b.get(att, 0.0))
    bnb  = b.get(nb, 0.0)
    r_base = bnb / batt

    if abil_norm:
        a_att = max(1e-9, abil_norm.get(att, 0.0))
        a_nb  = abil_norm.get(nb, 0.0)
        r_sig = a_nb / a_att  # ability比
    else:
        r_sig = r_base        # ability無なら base比で代用

    # グレード別ゲート（ability 主体）
    if event_grade in ("semi", "final", "graded"):   # 準優・優勝戦・重賞
        r_min, b1_max = 0.82, 0.78
    else:                                            # 一般戦
        r_min, b1_max = 0.90, 0.75

    # 3→4 は典型として少し緩和
    if att == 3 and nb == 4:
        r_min = max(0.0, r_min - 0.02)  # 0.80/0.88 など

    # prior を r_sig で滑らかにスケール（r=0.70→0.80, r=1.00→0.90）
    scale     = _clip((r_sig - 0.70) / 0.30, 0.0, 1.0)
    prior_rev = 0.80 + 0.10 * scale

    if (nb != att) and (r_sig >= r_min) and (b1 < b1_max):
        for k in thirds_excluding(nb, att):
            cand.append((f"{nb}-{att}-{k}", score(nb, att, k, prior=prior_rev), "成功/右隣同道(逆転)"))

    # ③ 成功/総括（頭=att 固定、2着は j_pref を厚め）
    for k in thirds_excluding(att, j_pref):
        cand.append((f"{att}-{j_pref}-{k}", score(att, j_pref, k, prior=0.90), "成功/総括"))

    # --- 裏スジ（攻め不発・ブロックされる） ---
    # ① 1残り右隣浮上: 1-nb-*
    if nb != 1:
        for k in thirds_excluding(1, nb):
            cand.append((f"1-{nb}-{k}", score(1, nb, k, prior=0.85), "裏スジ/1残り右隣"))

    # ② 保険（1-代替相手-総流し寄り）
    alt = att - 1 if att - 1 >= 2 else 2
    if alt != 1:
        for k in thirds_excluding(1, alt):
            cand.append((f"1-{alt}-{k}", score(1, alt, k, prior=0.75), "裏スジ/保険"))

    # --- 重複まとめ（上位スコア優先）＋3艇ユニーク保証 ---
    best: Dict[str, Tuple[str, float, str]] = {}
    for key, sc, tag in cand:
        a, b2, c = key.split("-")
        if len({a, b2, c}) != 3:
            continue
        if (key not in best) or (sc > best[key][1]):
            best[key] = (key, sc, tag)

    return list(best.values())


# ===== パブリックAPI =====
def generate_suji_tickets(
    base_probs: Dict[int, float],
    pairs: List[Pair],                     # 互換のため引数は残すが本版では未使用
    top_n_per_attacker: int = 10,          # 出し過ぎ防止のためデフォルト控えめ
    attackers_max: int = 2,
    threshold: float = 0.20,               # 基本の攻め艇スコア閾値
    event_grade: str = "normal",           # "normal" | "semi" | "final" | "graded"
    ability: Optional[Dict[int, float]] = None,  # 全国勝率などの地力指標（任意）
) -> List[Tuple[str, float, str]]:
    """
    返り値: [(買い目 'i-j-k', スコア, タグ), ...] をスコア降順。
    * スコアは 1-2着が支配的。3着は微小寄与のみ（運要素として弱く扱う）。
    * pairs(top2pair)は使わない。
    * event_grade で「右隣同道(逆転)」の出しやすさを調整。
    * ability（全国勝率など）を渡すと、逆転判定は ability 主体で行う。
    """
    # 攻め艇スコア
    scores = _attacker_scores(base_probs, ability=ability)

    # 1が強すぎる場合は攻め艇全体を減衰（“逃げ本線ならスジ舟券は控えめ”）
    b1 = base_probs.get(1, 0.0)
    if b1 > 0.70:
        factor = max(0.0, 1.0 - (b1 - 0.70))   # b1=0.80 → 0.90倍
        for k in scores:
            scores[k] *= factor

    # 攻め採用のしきい値を 1強度で自動加算（1が強いほど厳しく）
    dyn_thr = threshold + max(0.0, b1 - 0.70)

    # 閾値を超える攻め艇だけ採用
    attackers = [k for k in sorted(scores, key=scores.get, reverse=True)
                 if scores[k] >= dyn_thr][:attackers_max]

    if not attackers:
        return []

    # 候補収集
    all_cands: List[Tuple[str, float, str]] = []
    for att in attackers:
        all_cands.extend(_make_candidates_for(att, base_probs, ability=ability, event_grade=event_grade))

    # 重複まとめ（最大スコア採用）
    best: Dict[str, Tuple[str, float, str]] = {}
    for key, sc, tag in all_cands:
        if (key not in best) or (sc > best[key][1]):
            best[key] = (key, sc, tag)

    ranked = sorted(best.values(), key=lambda x: x[1], reverse=True)

    # “最低20本”は廃止。攻め艇数×top_n に比例して控えめに出す。
    limit = top_n_per_attacker * max(1, len(attackers))
    return ranked[:limit]


# ===== 参考：2連単だけの簡易生成（必要ならGUI側から呼べるサブ関数） =====
def generate_exacta_only(
    base_probs: Dict[int, float],
    attackers_max: int = 2,
    threshold: float = 0.20,
    event_grade: str = "normal",
    ability: Optional[Dict[int, float]] = None,
) -> List[Tuple[str, float, str]]:
    """
    2連単（i-j）に相当するキーを 'i-j-0' として返す簡易関数。
    GUI変更無しで「1-2着だけ見たい」用途に使える。
    """
    scores = _attacker_scores(base_probs, ability=ability)
    b1 = base_probs.get(1, 0.0)
    if b1 > 0.70:
        factor = max(0.0, 1.0 - (b1 - 0.70))
        for k in scores:
            scores[k] *= factor

    dyn_thr = threshold + max(0.0, b1 - 0.70)
    attackers = [k for k in sorted(scores, key=scores.get, reverse=True)
                 if scores[k] >= dyn_thr][:attackers_max]

    if not attackers:
        return []

    b = base_probs
    abil_norm = _normalize_ability(ability) if ability else None
    out: List[Tuple[str, float, str]] = []

    def s12(i: int, j: int, prior: float) -> float:
        bi = max(1e-9, b.get(i, 0.0)); bj = max(1e-9, b.get(j, 0.0))
        return prior * ((bi ** 0.85) * (bj ** 0.65))

    for att in attackers:
        nb = att + 1 if att < 6 else 6

        # 成功
        out.append((f"{att}-{1}-0",  s12(att, 1, 1.00),  "成功/差し(2連)"))
        if nb != att:
            out.append((f"{att}-{nb}-0", s12(att, nb, 0.95), "成功/右隣同道(2連)"))

        # 逆転（ability 主体で prior を滑らかに）
        if nb != att:
            if abil_norm:
                a_att = max(1e-9, abil_norm.get(att, 0.0))
                a_nb  = abil_norm.get(nb, 0.0)
                r_sig = a_nb / a_att
            else:
                batt = max(1e-9, b.get(att, 0.0))
                bnb  = b.get(nb, 0.0)
                r_sig = bnb / batt

            if event_grade in ("semi", "final", "graded"):
                r_min, b1_max = 0.82, 0.78
            else:
                r_min, b1_max = 0.90, 0.75
            if att == 3 and nb == 4:
                r_min = max(0.0, r_min - 0.02)

            scale     = _clip((r_sig - 0.70) / 0.30, 0.0, 1.0)
            prior_rev = 0.80 + 0.10 * scale

            if (r_sig >= r_min) and (b1 < b1_max):
                out.append((f"{nb}-{att}-0", s12(nb, att, prior_rev), "成功/右隣同道(逆転,2連)"))

        # 裏
        if nb != 1:
            out.append((f"1-{nb}-0",     s12(1, nb, 0.85), "裏スジ/1残り右隣(2連)"))
        alt = att - 1 if att - 1 >= 2 else 2
        if alt != 1:
            out.append((f"1-{alt}-0",    s12(1, alt, 0.75), "裏スジ/保険(2連)"))

    # 重複まとめ
    best: Dict[str, Tuple[str, float, str]] = {}
    for key, sc, tag in out:
        if (key not in best) or (sc > best[key][1]):
            best[key] = (key, sc, tag)

    ranked = sorted(best.values(), key=lambda x: x[1], reverse=True)
    limit = 6 * max(1, len(attackers))  # 2連はさらに軽く
    return ranked[:limit]
