# Playoff whistle report: plan

Goal: one consumable report (charts first, numbers second) on how the whistle changes in the playoffs, for whom, and why.

## Where we are (Nov 16)
- `po_whistle.py`: playoff compression by raw foul rate (top drawers −3%, bottom +6%; placebo flat). 2PT/3PT split only.
- `po_whistle2.py`: compression split into shot mix vs REAVES ratio. Shot mix enters only through the REGULAR-season xF.
- `po_retention.py`: shooting fouls keep 0.93 of their value in the playoffs vs placebo; FT% 1.09, shot-making 1.03,
  non-shooting fouls 1.15. 2/3 mix only.
- `po_falloff_players.csv`: per-player multi-season falloff (Iverson, Malone, World Peace clearest).
- **Gap: playoff shot location is not modeled.** Playoff expected fouls use the player's RS rate x playoff attempts,
  so a shift from drives to jumpers looks like a whistle change.

## Data available (shufinskiy repo)
- `shotdetail_po_1996..2025`: x/y and zone of every playoff FGA (same as RS `sd.py`).
- `datanba_po` (2016+), `cdnnba_po` (2020+): foul locations for fouled misses, as in RS `floc_link.py`.
- `nbastats_po` (parsed: `out_po/`), `matchups_po` (2017+).

## Build
1. `sd.py` on playoffs -> `sd_po/`; `floc_link.py` on playoffs (2016+); impute fouled-miss zones before 2016 (same psi).
2. Score playoff attempts with the RS xFoul model (zone x season, clock, margin, bonus, putback, defense decile; defense
   team-season FE -> the team's RS value; crews -> 0). Output `xfoul_att_po.parquet`.
3. Redo the three tests with real playoff xF: whistle change = fouls / xF in PO vs RS-vs-playoff-teams, per zone.
4. Decompose each player's playoff foul change into (a) shot-location shift (xF per attempt, PO vs RS) and
   (b) whistle per shot (fouls / xF). Same for league by season.

## Report sections
1. Is the playoff whistle tighter? League foul rate per attempt by zone, RS vs RS-vs-playoff-teams vs PO, by season.
2. Where shots go: shot density maps RS vs PO (league, and for case-study players), with the foul rate per zone.
3. Who loses calls: compression by REAVES / shot mix; retention of scoring components (fouls, shot-making, FT%, non-shooting).
4. Case studies: Harden, Luka, Embiid, Butler, Iverson, Malone; gainers Dirk, Kawhi, Tatum. Season-by-season charts.
5. Refs vs defenses: what the data can and can't separate (matchups 2017+: who guarded them in the playoffs).
6. Methods and caveats.
