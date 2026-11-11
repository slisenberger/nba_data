"""Parse nbastats play-by-play into possession-level rows with on-court lineups,
team-foul / penalty state, drawn-foul attribution and personal-foul counts.

Usage: python parse.py <season_start_year> <csv_path> <out_dir>
"""
import re
import sys
import numpy as np
import pandas as pd

TECH_FOUL = {10, 11, 12, 13, 16, 17, 18, 19, 25, 30, 32}  # technicals (11 T.FOUL, 32 flopping), double personal/tech, delay etc. Their (P#.T#) tag shows the count without adding to it (audit, Nov 2026)
OFF_FOUL = {4, 26}  # offensive / charge
SHOOT_FOUL = {2, 29}  # shooting, shooting block
TECH_FT = {16, 21, 22}
LAST_FT = {10, 12, 15}  # final FT of a regular trip (ends possession if made)
FLAG_FT = {18, 19, 20, 25, 26, 27, 28, 29}  # flagrant / clear path: offense keeps ball

ref_re = re.compile(r"\(([A-Z]\.[A-Za-z' -]+)\)\s*$")
foul_re = re.compile(r"\(P(\d+)(?:\.(T(\d+)|PN))?\)")
dist_re = re.compile(r"(\d+)'")


def clock(s):
    m, sec = s.split(":")
    return int(m) * 60 + float(sec)


def period_len(p):
    return 720 if p <= 4 else 300


def parse_game(g):
    """g: DataFrame of one game's events sorted. Returns (possessions list, foul events list, stint rows)."""
    gid = int(g.GAME_ID.iloc[0])
    # identify teams
    tids = pd.unique(pd.concat([g.PLAYER1_TEAM_ID, g.PLAYER2_TEAM_ID]).dropna())
    tids = [int(t) for t in tids if t > 0]
    if len(tids) != 2:
        return None
    # home team: PERSON1TYPE 4 is home player
    hp = g[(g.PERSON1TYPE == 4) & g.PLAYER1_TEAM_ID.notna()]
    if len(hp) == 0:
        return None
    home = int(hp.PLAYER1_TEAM_ID.mode().iloc[0])
    away = [t for t in tids if t != home][0]
    other = {home: away, away: home}

    rows = g.to_dict("records")
    poss = []
    fouls_out = []
    fgs = []  # FG attempts
    fts = []  # free throws (non-technical)
    refs = set()
    pf = {}  # personal fouls by player id
    lineup_end_prev = {home: [], away: []}
    for p in sorted(g.PERIOD.unique()):
        ev = [r for r in rows if r["PERIOD"] == p]
        # ---- starters for this period
        seen_first = {}  # pid -> ('in' or 'play')
        pteam = {}
        for r in ev:
            t = r["EVENTMSGTYPE"]
            if t == 8:
                po, pi = r["PLAYER1_ID"], r["PLAYER2_ID"]
                if po and po not in seen_first:
                    seen_first[po] = "play"; pteam[po] = r["PLAYER1_TEAM_ID"]
                if pi and pi not in seen_first:
                    seen_first[pi] = "in"; pteam[pi] = r["PLAYER1_TEAM_ID"]
                continue
            if t in (9, 11, 12, 13, 18, 7):
                continue
            if t == 6 and r["EVENTMSGACTIONTYPE"] in TECH_FOUL:
                continue
            if t == 3 and r["EVENTMSGACTIONTYPE"] in TECH_FT:
                continue
            for k in ("1", "2", "3"):
                if r[f"PERSON{k}TYPE"] in (4, 5) and r[f"PLAYER{k}_TEAM_ID"] == r[f"PLAYER{k}_TEAM_ID"] and r[f"PLAYER{k}_ID"]:
                    pid = r[f"PLAYER{k}_ID"]
                    if pid not in seen_first:
                        seen_first[pid] = "play"; pteam[pid] = r[f"PLAYER{k}_TEAM_ID"]
        on = {home: [], away: []}
        for pid, s in seen_first.items():
            tm = pteam.get(pid)
            if s == "play" and tm in (home, away):
                on[int(tm)].append(int(pid))
        quality = 1
        for tm in (home, away):
            if len(on[tm]) < 5:
                for pid in lineup_end_prev[tm]:
                    if len(on[tm]) >= 5:
                        break
                    if pid not in on[tm] and seen_first.get(pid) != "in":
                        on[tm].append(pid)
            if len(on[tm]) != 5:
                quality = 0
            on[tm] = on[tm][:5]

        # ---- walk events
        tf = {home: 0, away: 0}  # counting team fouls this period
        tf_last2 = {home: 0, away: 0}
        limit = 4 if p <= 4 else 3

        def in_pen(team, clk):
            return tf[team] >= limit or (clk <= 120 and tf_last2[team] >= 1)

        cur_off = None
        cur = None
        last_clk = period_len(p)
        last_def_foul = None  # info on most recent defensive foul for FT tagging
        prev_end_type = "period_start"

        def new_poss(off, clk):
            d = other[off]
            return dict(game_id=gid, period=p, off=off, deff=d, home_off=int(off == home),
                        start_clk=clk, end_clk=clk,
                        margin=score[off] - score[d],
                        def_tf=tf[d], def_pen=int(in_pen(d, clk)),
                        off_tf=tf[off],
                        start_type=prev_end_type,
                        off_lu=tuple(sorted(on[off])), def_lu=tuple(sorted(on[d])),
                        def_pf=tuple(pf.get(x, 0) for x in sorted(on[d])),
                        off_pf=tuple(pf.get(x, 0) for x in sorted(on[off])),
                        pts=0, pts_fg=0, pts_ft_shoot=0, pts_ft_bonus=0, pts_ft_other=0,
                        fga=0, fgm=0, fg3a=0, fg3m=0, rim_a=0, rim_m=0, tov=0, stl=0,
                        oreb=0, fta=0, d_shoot_fouls=0, d_nonshoot_fouls=0, d_nonshoot_pen=0,
                        lq=quality)

        score = {home: 0, away: 0} if p == 1 else score_carry
        for r in ev:
            t = r["EVENTMSGTYPE"]; a = r["EVENTMSGACTIONTYPE"]
            clk = clock(r["PCTIMESTRING"])
            h = r["HOMEDESCRIPTION"]; v = r["VISITORDESCRIPTION"]
            desc = (h if isinstance(h, str) else "") + "|" + (v if isinstance(v, str) else "")
            t1 = r["PLAYER1_TEAM_ID"]
            t1 = int(t1) if t1 == t1 and t1 else (int(r["PLAYER1_ID"]) if r["PLAYER1_ID"] in (home, away) else None)

            if t == 8:
                po, pi = int(r["PLAYER1_ID"]), int(r["PLAYER2_ID"]) if r["PLAYER2_ID"] else 0
                tm = t1
                if tm in on and po in on[tm] and pi:
                    on[tm][on[tm].index(po)] = pi
                elif tm in on and pi and pi not in on[tm] and len(on[tm]) < 5:
                    on[tm].append(pi)
                continue

            if t == 6:
                if a in TECH_FOUL or t1 not in (home, away):
                    continue
                rm = ref_re.search(desc.replace('|', ' ').strip())
                if rm:
                    refs.add(rm.group(1).strip())
                m = foul_re.search(desc)
                fouler = int(r["PLAYER1_ID"]) if r["PLAYER1_ID"] else 0
                if m and fouler:
                    pf[fouler] = int(m.group(1))
                elif fouler and r["PERSON1TYPE"] in (4, 5):
                    pf[fouler] = pf.get(fouler, 0) + 1
                counting = bool(m and m.group(2)) and a not in OFF_FOUL
                if counting:
                    # A counting foul by t1 means the other team has (or now gets) the ball. If the possession row
                    # hasn't switched yet (the foul is the fouled team's first logged event: a fouled miss after a
                    # made basket, a loose-ball foul on a rebound), open it here, so its team-foul / penalty state
                    # is stamped BEFORE this foul and the foul is counted in it. (Until Nov 2026 the row opened at
                    # the following FT, after tf was incremented: 9% of possessions had an inflated def_tf.)
                    if cur is None or cur["off"] != other[t1]:
                        if cur is not None:
                            cur["end_clk"] = last_clk
                            poss.append(cur)
                        cur = new_poss(other[t1], last_clk if cur is not None else period_len(p))
                        cur_off = other[t1]
                    pen_before = in_pen(t1, clk)
                    tf[t1] += 1
                    if clk <= 120:
                        tf_last2[t1] += 1
                    drawn_by = int(r["PLAYER2_ID"]) if r["PLAYER2_ID"] and r["PERSON2TYPE"] in (4, 5) else 0
                    fe = dict(game_id=gid, period=p, clk=clk, fouler_team=t1, drawer_team=other[t1],
                              fouler=fouler, drawn_by=drawn_by, action=a, pen_before=int(pen_before),
                              pn_flag=int(m.group(2) == "PN"), tf_after=tf[t1],
                              fouler_pf=pf.get(fouler, 0),
                              drawer_lu=tuple(sorted(on[other[t1]])), fouler_lu=tuple(sorted(on[t1])),
                              ft_shooter=0, evnum=int(r['EVENTNUM']), shoot=int(a in SHOOT_FOUL))
                    fouls_out.append(fe)
                    last_def_foul = fe
                    if cur is not None and cur["off"] == other[t1]:
                        if a in SHOOT_FOUL:
                            cur["d_shoot_fouls"] += 1
                        else:
                            cur["d_nonshoot_fouls"] += 1
                            if pen_before:
                                cur["d_nonshoot_pen"] += 1
                continue

            # offense-defining events
            off = None
            if t in (1, 2, 5):
                off = t1
            elif t == 3:
                if a in TECH_FT:
                    if "MISS" not in desc and t1 in score:
                        score[t1] += 1
                    continue
                off = t1
            elif t == 4:
                off = t1
            if off not in (home, away):
                continue
            if off != cur_off:
                if cur is not None:
                    cur["end_clk"] = last_clk
                    poss.append(cur)
                    # end type of previous possession
                cur = new_poss(off, last_clk if cur is not None else period_len(p))
                cur_off = off
            if t == 4:
                if cur["fga"] > 0 or cur["fta"] > 0:
                    cur["oreb"] += int(r["PERSON1TYPE"] in (4, 5))
                last_clk = clk
                continue
            if t in (1, 2):
                is3 = "3PT" in desc
                dm = dist_re.search(desc)
                dist = int(dm.group(1)) if dm else None
                rim = (dist is not None and dist <= 3) or any(k in desc for k in ("Layup", "Dunk", "Tip"))
                cur["fga"] += 1; cur["fg3a"] += int(is3); cur["rim_a"] += int(rim and not is3)
                fgs.append(dict(period=p, clk=clk, evnum=int(r["EVENTNUM"]), off=off, shooter=int(r["PLAYER1_ID"]),
                                made=int(t == 1), is3=int(is3), dist=dist if dist is not None else -1, fg_action=int(a),
                                pidx=len(poss), elapsed=cur["start_clk"] - clk, margin=score[off] - score[other[off]],
                                def_tf=tf[other[off]], def_pen=int(in_pen(other[off], clk)), start_type=cur["start_type"],
                                def_lu=cur["def_lu"], off_lu=cur["off_lu"], blk=int(r["PERSON3TYPE"] in (4, 5) and r["PLAYER3_ID"] > 0)))
                if t == 1:
                    pts = 3 if is3 else 2
                    cur["fgm"] += 1; cur["fg3m"] += int(is3); cur["rim_m"] += int(rim and not is3)
                    cur["pts"] += pts; cur["pts_fg"] += pts; score[off] += pts
                    prev_end_type = "made"
            elif t == 3:
                cur["fta"] += 1
                made = "MISS" not in desc
                fts.append(dict(period=p, clk=clk, off=off, shooter=int(r["PLAYER1_ID"]), ft_action=int(a), made=int(made),
                                evnum=int(r["EVENTNUM"]), pidx=len(poss), elapsed=cur["start_clk"] - clk,
                                margin=score[off] - score[other[off]], def_tf=tf[other[off]], def_pen=int(in_pen(other[off], clk)),
                                start_type=cur["start_type"], def_lu=cur["def_lu"], off_lu=cur["off_lu"]))
                if last_def_foul is not None and last_def_foul["drawer_team"] == off and last_def_foul["ft_shooter"] == 0:
                    last_def_foul["ft_shooter"] = int(r["PLAYER1_ID"])
                if made:
                    cur["pts"] += 1; score[off] += 1
                    lf = last_def_foul
                    if lf is not None and lf["drawer_team"] == off and lf["action"] in SHOOT_FOUL:
                        cur["pts_ft_shoot"] += 1
                    elif lf is not None and lf["drawer_team"] == off and lf["pen_before"]:
                        cur["pts_ft_bonus"] += 1
                    else:
                        cur["pts_ft_other"] += 1
                    if a in LAST_FT:
                        prev_end_type = "made"
            elif t == 5:
                cur["tov"] += 1
                if r["PERSON2TYPE"] in (4, 5) and r["PLAYER2_ID"]:
                    cur["stl"] += 1
                    prev_end_type = "steal"
                else:
                    prev_end_type = "deadball_tov"
            last_clk = clk
            if t == 2 or (t == 3 and "MISS" in desc):
                prev_end_type = "miss"
        if cur is not None:
            cur["end_clk"] = 0.0
            poss.append(cur)
        score_carry = score
        lineup_end_prev = {k: list(v) for k, v in on.items()}
    # starting types: the type is the end of the previous possession by the OTHER team; prev_end_type logic is
    # approximate: 'miss' preceding a change of possession means a defensive rebound start.
    att = build_attempts(gid, fgs, fts, fouls_out, other)
    return poss, fouls_out, att, sorted(refs)


TRIP = {10: 1, 11: 2, 13: 3}  # first FT of a regular trip -> trip size
TRIP_NEXT = {12, 14, 15}


def build_attempts(gid, fgs, fts, fouls, other):
    """One row per shooting attempt: every FGA (and-ones flagged) + every fouled miss (2- or 3-FT shooting trip).
    FT trips are linked to the latest unused counting foul by the other team at the same clock; a trip counts as a
    shooting attempt if that foul is a shooting foul. A 1-FT trip is an and-one, matched to a made FG at that clock."""
    rows = []
    for f in fgs:
        rows.append(dict(f, fouled=0, ft_trip=0, ftm=0, fta=0, fouler=0, foul_action=0, kind="fga"))
    used_fouls = set(); used_fg = set()
    trip = None
    fbyk = {}
    for i, fo in enumerate(fouls):
        fbyk.setdefault((fo["period"], fo["clk"], fo["drawer_team"]), []).append(i)
    fgbyk = {}
    for i, f in enumerate(fgs):
        if f["made"]:
            fgbyk.setdefault((f["period"], f["clk"], f["off"]), []).append(i)
    unmatched = 0
    for ft in fts:
        a = ft["ft_action"]
        if a in TRIP:
            trip = None
            k = (ft["period"], ft["clk"], ft["off"])
            cands = [i for i in fbyk.get(k, []) if i not in used_fouls]
            if not cands:
                row = dict(ft, made=0, is3=0, dist=-1, fg_action=0, blk=0, fouled=0, ft_trip=TRIP[a], ftm=0, fta=0,
                           fouler=0, foul_action=-1, kind="unlinked_trip")
                row.pop("ft_action"); rows.append(row)
                row["fta"] += 1; row["ftm"] += ft["made"]; trip = row
                continue
            sh = [i for i in cands if fouls[i]["shoot"]]
            fi = (sh or cands)[-1]
            used_fouls.add(fi)
            fo = fouls[fi]
            if not fo["shoot"]:
                row = dict(ft, made=0, is3=0, dist=-1, fg_action=0, blk=0, fouled=0, ft_trip=TRIP[a], ftm=0, fta=0,
                           fouler=fo["fouler"], foul_action=fo["action"], kind="ns_trip")
                row.pop("ft_action"); rows.append(row)
                row["fta"] += 1; row["ftm"] += ft["made"]; trip = row
                continue
            n = TRIP[a]
            if n == 1:
                fc = [i for i in fgbyk.get(k, []) if i not in used_fg]
                pref = [i for i in fc if fgs[i]["shooter"] == ft["shooter"]]
                if not fc:
                    row = dict(ft, made=0, is3=0, dist=-1, fg_action=0, blk=0, fouled=1, ft_trip=1, ftm=0, fta=0,
                               fouler=fo["fouler"], foul_action=fo["action"], kind="and1_unmatched")
                    row.pop("ft_action"); rows.append(row)
                    row["fta"] += 1; row["ftm"] += ft["made"]; trip = row
                    continue
                gi = (pref or fc)[-1]; used_fg.add(gi)
                row = rows[gi]
                row.update(fouled=1, ft_trip=1, fouler=fo["fouler"], foul_action=fo["action"], kind="and1")
            else:
                row = dict(ft, made=0, is3=int(n == 3), dist=-1, fg_action=0, blk=0, fouled=1, ft_trip=n, ftm=0, fta=0,
                           fouler=fo["fouler"], foul_action=fo["action"], kind="fouled_miss")
                row.pop("ft_action")
                rows.append(row)
            row["fta"] += 1; row["ftm"] += ft["made"]
            trip = row
        elif a in TRIP_NEXT and trip is not None and ft["off"] == trip["off"]:
            trip["fta"] += 1; trip["ftm"] += ft["made"]
        else:
            trip = None
            row = dict(ft, made=0, is3=0, dist=-1, fg_action=0, blk=0, fouled=0, ft_trip=0, ftm=ft["made"], fta=1,
                       fouler=0, foul_action=-1, kind="other_ft")  # flagrant / clear path / stray
            row.pop("ft_action"); rows.append(row)
    for r in rows:
        r["game_id"] = gid
        r["deff"] = other[r["off"]]
        r["pts"] = r["made"] * (3 if r["is3"] else 2) + r["ftm"]
    return rows


def main(year, path, out):
    d = pd.read_csv(path, low_memory=False)
    d = d.sort_values(["GAME_ID", "PERIOD", "EVENTNUM"])  # EVENTNUM order
    for c in ("PLAYER1_ID", "PLAYER2_ID", "PLAYER3_ID"):
        d[c] = d[c].fillna(0).astype("int64")
    for c in ("PERSON1TYPE", "PERSON2TYPE", "PERSON3TYPE"):
        d[c] = d[c].fillna(0).astype(int)
    allp, allf, alla, allr = [], [], [], []
    for gid, g in d.groupby("GAME_ID", sort=False):
        res = parse_game(g)
        if res is None:
            continue
        allp.extend(res[0]); allf.extend(res[1]); alla.extend(res[2])
        allr.append(dict(game_id=int(gid), refs="|".join(res[3])))
    P = pd.DataFrame(allp); F = pd.DataFrame(allf); A = pd.DataFrame(alla)
    for df in (P, F, A):
        df["season"] = year
    for c in ("off_lu", "def_lu"):
        A[c] = A[c].map(lambda x: list(x))
    A.to_parquet(f"{out}/att_{year}.parquet")
    pd.DataFrame(allr).to_parquet(f"{out}/refs_{year}.parquet")
    for c in ("off_lu", "def_lu", "def_pf", "off_pf"):
        P[c] = P[c].map(lambda x: list(x))
    for c in ("drawer_lu", "fouler_lu"):
        F[c] = F[c].map(lambda x: list(x))
    P.to_parquet(f"{out}/poss_{year}.parquet")
    F.to_parquet(f"{out}/fouls_{year}.parquet")
    # player names
    names = pd.concat([d[["PLAYER1_ID", "PLAYER1_NAME"]].rename(columns={"PLAYER1_ID": "pid", "PLAYER1_NAME": "name"}),
                       d[["PLAYER2_ID", "PLAYER2_NAME"]].rename(columns={"PLAYER2_ID": "pid", "PLAYER2_NAME": "name"})])
    names = names[names.pid > 0].dropna().drop_duplicates("pid")
    names.to_parquet(f"{out}/names_{year}.parquet")
    print(year, len(P), len(F), round(P.lq.mean(), 4), flush=True)


if __name__ == "__main__":
    main(int(sys.argv[1]), sys.argv[2], sys.argv[3])
