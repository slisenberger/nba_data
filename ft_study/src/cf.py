"""Counterfactual penalty-state analysis.
For every (game, period, fouling team) recompute the opponent's penalty status per possession,
then (a) remove each single counting foul and (b) remove all fouls drawn by each drawer,
counting penalty possessions that disappear, split by whether the drawer was on the floor.
Also computes fouler's remaining on-court possessions after each foul (foul-trouble channel)."""
import sys, numpy as np, pandas as pd
from collections import defaultdict

SHOOT = {2, 29}
rng = np.random.default_rng(7)


def pen_status(fclk, starts, period):
    """fclk: sorted array of foul clocks (seconds remaining) for the defense; starts: possession start clocks.
    A foul counts for a possession if it happened strictly before it (clk > start)."""
    limit = 4 if period <= 4 else 3
    fclk = np.asarray(fclk, dtype=float)
    if len(fclk) == 0:
        return np.zeros(len(starts), dtype=bool)
    cnt = (fclk[None, :] > starts[:, None]).sum(1)
    last2 = ((fclk[None, :] > starts[:, None]) & (fclk[None, :] <= 120)).sum(1)
    return (cnt >= limit) | ((starts <= 120) & (last2 >= 1))


def main(year, ratio_path, out):
    P = pd.read_parquet(f"/home/claude/data/out/poss_{year}.parquet",
                        columns=["game_id", "period", "off", "deff", "start_clk", "def_pen", "off_lu", "def_lu", "lq"])
    F = pd.read_parquet(f"/home/claude/data/out/fouls_{year}.parquet")
    P["off_lu"] = P.off_lu.map(lambda x: set(int(v) for v in x))
    P["def_lu"] = P.def_lu.map(lambda x: set(int(v) for v in x))

    # ---------- attribution
    F["drawer"] = F.drawn_by.where(F.drawn_by > 0, 0)
    sp = F.action.isin(SHOOT) | (F.pen_before == 1)
    F.loc[(F.drawer == 0) & sp & (F.ft_shooter > 0), "drawer"] = F.ft_shooter
    F["imputed"] = 0
    if (F.drawn_by > 0).mean() < 0.5:
        ratio = pd.read_parquet(ratio_path).set_index("pid").r
        r_league = ratio.attrs.get("league", None) or float(ratio.median())
        # per-player shoot+pen drawn counts this season, normalised by on-court offensive possessions
        on_cnt = defaultdict(int)
        for lu in P.off_lu:
            for x in lu:
                on_cnt[x] += 1
        sp_cnt = F[sp & (F.drawer > 0)].drawer.value_counts()
        rate = {pid: sp_cnt.get(pid, 0) / max(on_cnt[pid], 1) for pid in on_cnt}
        idx = F.index[(F.drawer == 0)]
        for i in idx:
            lu = [int(x) for x in F.at[i, "drawer_lu"]]
            if not lu:
                continue
            w = np.array([rate.get(x, 0) * ratio.get(x, r_league) for x in lu]) + 1e-6
            F.at[i, "drawer"] = lu[rng.choice(len(lu), p=w / w.sum())]
            F.at[i, "imputed"] = 1

    # ---------- per game: possession index for remaining-possession counts
    single = np.zeros(len(F)); single_on = np.zeros(len(F)); rem_poss = np.full(len(F), np.nan)
    joint = defaultdict(lambda: [0, 0, 0])  # (drawer) -> [flips_on, flips_off, fouls]
    agree = [0, 0]
    Fg = F.groupby("game_id").indices
    for gid, pg in P.groupby("game_id", sort=False):
        if gid not in Fg:
            continue
        fg = F.iloc[Fg[gid]]
        # remaining on-court possessions for each foul's fouler (both ends)
        order = (pg.period.to_numpy() * 10000 - pg.start_clk.to_numpy())  # increasing in game time
        lus = [a | b for a, b in zip(pg.off_lu, pg.def_lu)]
        for fi, row in zip(Fg[gid], fg.itertuples()):
            t = row.period * 10000 - row.clk
            after = order > t
            rem_poss[fi] = sum(1 for k in np.nonzero(after)[0] if row.fouler in lus[k])
        for (per, D), ff in fg.groupby(["period", "fouler_team"]):
            ps = pg[(pg.period == per) & (pg.off != D)]
            if len(ps) == 0:
                continue
            starts = ps.start_clk.to_numpy(float)
            base = pen_status(ff.clk.to_numpy(), starts, per)
            agree[0] += int((base == ps.def_pen.to_numpy().astype(bool)).sum()); agree[1] += len(ps)
            onlists = list(ps.off_lu)
            # single-foul removal
            clks = ff.clk.to_numpy()
            locs = [F.index.get_loc(x) for x in ff.index]
            for j, fi in enumerate(locs):
                cf = pen_status(np.delete(clks, j), starts, per)
                flip = base & ~cf
                single[fi] = flip.sum()
                d = F.iat[fi, F.columns.get_loc("drawer")]
                single_on[fi] = sum(1 for k in np.nonzero(flip)[0] if d in onlists[k])
            # joint removal per drawer
            drs = ff.drawer.to_numpy()
            for d in set(drs):
                if d == 0:
                    continue
                keep = drs != d
                cf = pen_status(clks[keep], starts, per)
                flip = np.nonzero(base & ~cf)[0]
                on_ = sum(1 for k in flip if d in onlists[k])
                j3 = joint[int(d)]
                j3[0] += on_; j3[1] += len(flip) - on_; j3[2] += int((~keep).sum())
    F["flips_single"] = single; F["flips_single_on"] = single_on; F["rem_poss"] = rem_poss
    F.drop(columns=["drawer_lu", "fouler_lu"]).to_parquet(f"{out}/cf_fouls_{year}.parquet")
    # player-season summary: on-court offensive possessions, team offensive possessions
    on_cnt = defaultdict(int); team_of = {}
    for lu, off in zip(P.off_lu, P.off):
        for x in lu:
            on_cnt[x] += 1; team_of[x] = off
    J = pd.DataFrame([dict(pid=k, flips_on=v[0], flips_off=v[1], drawn_counting=v[2]) for k, v in joint.items()])
    O = pd.DataFrame([dict(pid=k, off_poss=v) for k, v in on_cnt.items()])
    S = O.merge(J, on="pid", how="left").fillna(0)
    S["season"] = year
    S.to_parquet(f"{out}/cf_players_{year}.parquet")
    print(year, "pen agreement", round(agree[0] / agree[1], 4), "imputed", int(F.imputed.sum()), flush=True)


if __name__ == "__main__":
    main(int(sys.argv[1]), sys.argv[2], sys.argv[3])
