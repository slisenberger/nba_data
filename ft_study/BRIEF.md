# Free-throw / foul-drawing study — handoff brief

## Goal
A full study of NBA foul drawing (1996–97 to present) built on play-by-play:
1. Better rate stats than FTr and TS%: they handle and-ones badly and don't count shooting fouls as shot attempts.
2. **REAVES** (Relative Efficiency Added from Violating Ethical Shotmaking): shooting fouls drawn above what's expected for the shot's context, valued in points.
3. A **foul EPA**: the change in expected points from a drawn foul in its game state, covering free throws, moving toward the bonus, foul trouble, and any contextual lift.
4. All-time rankings of foul drawers by impact, plus a way to spot "grifters."

## What already exists (src/)
- `parse.py` — nbastats PBP → one row per possession (both lineups, team fouls, penalty state, personal fouls, outcome split into FG / shooting-foul FT / bonus FT) + a foul-event table (who fouled, who drew it, action type, penalty flag). Penalty state matches the league's PN tags 98% of the time.
- `runall.sh` — runs the parser per season. Data: github.com/shufinskiy/nba_data (`datasets/nbastats_<year>.tar.xz`). Fetch with a sparse git checkout; raw URLs are blocked by some proxies.
- `cf.py` — counterfactual foul timelines: bonus possessions created by each foul, and by each player's fouls taken together, split by whether he was on or off the floor. Also each fouler's possessions remaining after the foul. Imputes who drew the foul for 1996–2004, when the PBP doesn't name the fouled player.
- `fe.py` — lean multi-way fixed-effects OLS (alternating projections) with game-clustered SEs. pyfixest runs out of memory at 8 GB.
- `a_bonus.py`, `a_eff.py` — bonus effect: 3→4 team-foul threshold contrast with 1→2 and 2→3 placebos.
- `q_minutes.py` — possessions lost per nth personal foul, by quarter (player-season FE).
- `q3.py` — defender foul-trouble hesitance (defender-season FE). The game-FE version didn't finish.
- `starter.py`, `delta.py` — value of a top-5 player per possession (0.0144).
- `star.py`, `ftm.py` — player-season and career attribution tables.

## Findings so far (see res_*.csv, career.csv)
- Bonus-specific value: +0.012 pts per possession (raw threshold contrast 0.027, naive FE 0.066). All of it comes from the bonus FTs themselves. No sign of a better scoring environment or hesitant defense.
- Each team foul goes with +0.015 pts/poss even below the bonus. So far this is treated as whistle environment, not something the foul causes. **Test this first:** rerun the placebo steps with game FE. Whether it's causal changes star bench value by about 3–4×.
- One team foul creates 1.86 opponent bonus possessions on average. A 4th foul early in a quarter creates about 4.5.
- Foul trouble: a Q1 2nd foul costs ~10 possessions (both ends combined), a Q2 3rd foul ~8, a Q4 foul ~0.3–5.
- Shaq / Embiid / Howard: #1 / #2 / #4 in team fouls drawn per 100 on-court possessions; bench bonus value 0.03–0.11 pts/game.

## Proposed next work
1. **Shot-attempt accounting** from PBP. Shooting attempts = FGA + fouled missed shots (2- or 3-FT trips). Count an and-one once, as a made FGA plus a foul. New stats: foul-drawn rate per attempt, and-one rate, FT trips/100, and *exact* points per shooting attempt, replacing TS%'s 0.44·FTA approximation.
2. **xFoul model** for REAVES: P(shooting foul | shot type and distance, zone, transition/half-court, score/time, era, referee crew, defender). Known problem: a fouled miss has no shot location. Options: take shot type from the foul or FT context, borrow the player's location mix, or use shotdetail data for made and-ones only. REAVES = (fouls − xFouls) × (value of a foul − value of the expected shot).
3. **Foul EPA**: expected-points-by-state model (period, clock, score, team fouls, personal fouls of the five defenders, bonus) → EP(after foul) − EP(before). It absorbs both the bonus and foul-trouble channels; use game FE to separate cause from whistle environment.
4. **Grifter identification**: the 2021–22 "non-basketball move" rule change is a natural experiment. Compare each player's change in foul rate on jump shots and 3PA against xFoul. Large drops mark players who relied on rip-throughs and leg kicks. A 2nd signal: high foul rate on low-xFoul shot types.
5. Add 2025–26 (nbastatsv3 files) and playoffs.

## Data caveats
- 1996–2004: the fouled player isn't recorded, so ~35% of team fouls are imputed.
- Lineup inference is clean for ~94% of period-lineups.
- Pre-1996 (e.g., Shaq in Orlando) has no PBP.
