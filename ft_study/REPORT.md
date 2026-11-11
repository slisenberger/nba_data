# Foul-drawing study: findings (Nov 2026)

Continues `BRIEF.md`. Data: nbastats play-by-play 1996-97 to 2024-25, plus 2025-26 (converted from the v3 format)
and playoffs 1996-2024; shot detail for locations. Regular season unless stated. Seasons are labelled by start
year in the CSVs (2018 = 2018-19). Pipeline and file map: `README.md`.

## 0. The handoff's "+0.015 per team foul" was a coding artifact (priority test)

**Answer: not causal, and not whistle environment either. The parser created it.**

`parse.py` opened a possession row at the offense's first *logged* event. When that event is the defense's foul
(a fouled miss right after a made basket, a loose-ball foul on the rebound), the row opened at the following free
throw, after the team-foul counter had already gone up. The foul wasn't counted in the possession, but its FT
points were. **9.3% of possessions carried an inflated team-foul count**, and these were exactly the possessions
that start with FTs. The step comparisons picked that up as "each foul adds +0.015".

Evidence (`results/res_bonus_gfe.csv` handoff coding, `results/res_bonus_fix.csv` corrected; pts per possession, SE):

| step | handoff coding, orig FE | + game x defense FE | **corrected, orig FE** | **corrected, game x defense FE** |
|---|---|---|---|---|
| 0→1 | +0.039 (.002) | +0.039 (.002) | **+0.001** (.002) | −0.006 (.002) |
| 1→2 | +0.017 (.002) | +0.017 (.002) | **+0.000** (.002) | −0.006 (.002) |
| 2→3 | +0.013 (.002) | +0.012 (.002) | **+0.004** (.002) | −0.005 (.002) |
| 3→4 (bonus) | +0.027 (.002) | +0.025 (.003) | **+0.039** (.002) | **+0.028** (.003) |

- Game FE didn't move the handoff numbers: the slope was not a game-level whistle environment. Under the handoff
  coding its components were FT points up and FG points down, as you'd expect from mislabelled FT possessions.
- An **event study** (`a_event.py`, `results/res_event.csv`) gives the same answer. Take 97k clean pre-bonus fouls
  (no other defensive foul within ±4 possessions), subtract the league scoring curve by period-minute, and
  demean within event. The defense's next 4 possessions are −0.012 vs the −4/−3 baseline (SE .003). There is no
  step up after a foul, so any causal effect is at most about +0.005.
- **Fixed in `parse.py`** (a defensive foul now opens the fouled team's possession). After a full rebuild, def_tf
  matches a clock-based recount for 99.5% of possessions (90.5% before). Penalty state matches `cf.py`'s
  clock-based reconstruction for 99.9% of possessions (98% in the handoff).

**Consequences**
- The **bonus is worth more** than the handoff said. The handoff subtracted the artifact slope to get 0.012.
  Corrected: **β = 0.035 pts per possession in the penalty** (range 0.028 with game FE to 0.039), against a
  1.08 base. Bonus FTs add +0.12 to +0.13; possessions that end at the line lose −0.05 of FG points.
  Possessions in the penalty with no foul score +0.001 to +0.011, so there is still no sign of hesitant defense.
- **Star bench value is ~3× the handoff estimate**, but because the bonus is worth more, not because of a slope.
  (A causal slope would have meant 3–4×.) Bench bonus value per game, β = 0.035: Giannis 0.16, Embiid 0.14,
  Shaq 0.13, Howard 0.10 (`results/bench_career.csv`, with low/high β columns). Career totals: Giannis 140,
  Howard 126, Shaq 122, Dirk 122 pts.
- Handoff scripts that use `def_tf` / `def_pen` (`a_bonus.py`, `a_eff.py`, `q3.py`) were run on the old coding.
  Their old outputs in `results/handoff/` are superseded where they touch team-foul state. `cf.py`,
  `q_minutes.py` and `delta.py` use clocks or lineups and are unaffected.

## 1. Shot-attempt accounting (`shots.py`, `results/shots_*.csv`)

A shooting attempt (SA) is an FGA plus a fouled miss (a 2- or 3-FT shooting trip); an and-one counts once.
Every FT and every point reconciles exactly against the possession table. Only 1 and-one in ~6,500 per season
fails to match a made FG.

- **About 1 in 9 shooting attempts is fouled** (10–12% every season since 1996). About 23% of shooting fouls are
  and-ones (21% in the 1990s, 25% around 2021).
- **3PA foul rate:** 0.6% in the early 2000s → peak **1.7% in 2016-17 to 2019-20** → **1.1% after the 2021-22
  rule change** (0.95% in 2023-24), back to 1.3% in 2025-26.
- **Exact points per shooting attempt** in 2024-25 was 1.14 (1.05 in 1996-97). The exact TS-style rate counts
  each non-shooting FT trip as one attempt and excludes technicals.
- **TS%'s 0.44 is close.** The implied coefficient is 0.45 in the 1990s and 0.435–0.44 since 2016. Classic TS
  is within ±1 point of the exact rate for every career, and the error tracks and-one share (r = 0.74): heavy
  and-one finishers are slightly under-rated (Harrell, Zion, Whiteside +0.4 to +0.7), and big men who get
  hacked in the bonus slightly over-rated (Reggie Evans −1.0). The new information is the per-attempt foul rate,
  not a corrected TS.
- **Highest foul rate per attempt** (min 3,000 SA): Dwight Howard 26.5%, Reggie Evans 26.1%, Gobert 24.5%,
  Tyson Chandler 22.7%, Shaq 22.6%, Zion 21.8%, Giannis 21.3%. **Most fouled attempts above league rate:**
  Howard +2,326, Shaq +1,872, Giannis +1,785, Harden +1,620, LeBron +1,545.
- Playoffs: `results/shots_*_po.csv`. LeBron, Duncan, Howard, Giannis and Harden follow Shaq (+397).

## 2. xFoul and REAVES (`xfoul.py`, `reaves.py`)

**The missing-location problem.** A fouled miss logs no FGA, so it has no location. For each season and 2PT zone
(restricted area / paint / mid-range): fouled misses in a zone = and-ones in that zone × (1−ψ)/ψ, where ψ is the
chance a fouled shot still goes in. ψ = κ × FG% of unfouled shots in the zone. κ is *observed* for jump shots on
threes (4-point plays vs 3-FT trips: pooled κ = 0.58) and applied to the mid-range. For the restricted area and
paint, κ is solved each season so the zones add up to the observed 2PT fouled misses (κ ≈ 0.38–0.47: contact at
the rim hurts more). Each player-season's fouled misses are spread over zones by his own and-one mix (shrunk with
10 pseudo and-ones toward the mix implied by his FGA locations). Implied foul rates per try: restricted area
~18–24%, paint 15–19%, mid-range 3–8%, threes ~1–2%.

**The model.** A linear probability model with fixed effects for zone × season, zone × possession age × start
type (transition), zone × period × clock, zone × margin, zone × bonus state, defense team-season × 2/3, and zone
× decile of the on-court defenders' shooting-foul rate. Referee crews are added from a ridge fit where the
crew is named (2015+, 112 refs). Ref effects are small: SD 0.15 pp per attempt, 0.24 pp per crew. 6.5M
attempts, well calibrated: predicted vs observed foul rate in each band is .009/.011, .032/.031, .064/.067,
.101/.102, .161/.155, .243/.241, .368/.376.

**REAVES** = Σ (fouled − xF) × (league value of a fouled attempt − unfouled attempt from that zone) + (fouls − xF)
× downstream value per shooting foul (bonus + opponent foul trouble, ~0.08). It is valued at league FT%, so it
measures drawing, not FT shooting. `results/reaves_career.csv`, `reaves_seasons.csv`.

| career REAVES (pts) | | per 100 attempts (min 3,000) | |
|---|---|---|---|
| James Harden | +1,981 (1.76× expected fouls) | Harden | 8.6 |
| DeMar DeRozan | +1,278 | Embiid | 7.6 |
| Dirk Nowitzki | +1,115 | Butler | 7.0 |
| Kevin Durant | +956 | Dončić | 6.4 |
| Jimmy Butler | +952 | Howard | 6.3 |
| Dwight Howard | +947 | Maggette, K. Martin, Gobert, DeRozan | 5.5–5.7 |

Bottom: Shawn Marion −681, Vučević −637, Thaddeus Young −589, Payton, Holiday, Nash, Rondo; T.J. McConnell
−8.6 per 100. Best seasons: Harden 2018-19 (+230; 358 fouls vs 176 expected), Harden 2016-17 and 2019-20,
Embiid 2021-22 (+159), Harden 2017-18. Shaq is "only" +554: his fouls were 1.25× expected because rim attempts
*expect* fouls. His impact shows up in EPA and bench value, not in REAVES. (Austin Reaves: +4.6 per 100, 19th
of 705.)

## 3. Foul EPA (`foul_epa.py`, `results/foul_epa_*.csv`)

EPA per drawn foul = immediate + bonus + foul trouble. The whistle-slope term is 0 (section 0).
- Immediate: for a shooting foul, the fouled attempt's points at league FT% minus the league value of an
  unfouled FGA from that zone. For a non-shooting foul, E[possession points | such a foul at this possession
  age] minus E[points | possession still alive at that age].
- Bonus: opponent bonus possessions the foul creates (cf.py single-foul removal) × β = 0.035.
- Trouble: possessions the fouler loses (res_minutes) × δ = 0.0144, for top-5 foulers only.

Average EPA: **shooting foul +0.84** (0.76 immediate, 0.07 bonus, 0.01 trouble). **Non-shooting foul in the
bonus +0.29.** **Non-shooting foul before the bonus +0.09**, almost all of it the bonus it moves the team toward.

Career foul EPA: LeBron 5,312, Harden 4,151, Kobe 4,136, Shaq 3,951, Duncan 3,902, DeRozan 3,807, Howard 3,707,
Durant 3,577. **Per 100 possessions: Shaq 6.6, Embiid 6.0, Dončić 5.7, Giannis 5.3, Amar'e 5.2, Cousins 5.0,
Malone 5.0, Harden 4.8.** Per game: Shaq 4.3, Dončić 4.1, Embiid 4.0.

## 4. Grifters (`grifter.py`, `results/grifter_*.csv`)

**The rule-change natural experiment.** Compare 3PA fouls over expected per 100 attempts, 2019-21 vs 2021-23,
for players with ≥300 3PA in both windows. xF absorbs the league-wide drop. Normal regression to the mean
predicts a post-on-pre slope of 0.67 (placebo: 2016-18 → 2018-20). Across the rule change the slope is
**0.51**: the players who drew the most fouls on threes lost more than regression explains.
- Biggest drops on 3s: **Trae Young −3.8 per 100 (4.8 → 1.0; z = −7.1)**, Terrence Ross −3.3, Quickley −2.7,
  Dinwiddie −2.6, Ingram −2.3, Bertāns −2.3, Dončić −1.8.
- On mid-range jumpers: **Harden −17 per 100** (19.8 → 2.6), Trae −10, Dinwiddie −9.7. Harden's 3PA foul rate
  didn't fall (+0.5). The rule caught his mid-range and drive game, not his threes.
- Most fouls lost per season on low-xF shots: Trae ~96, Dinwiddie 42, Fox 27, Dončić 24.

**Career foul drawing on low-xF shots** (3PA + mid-range, shrunk; per 100): Harden 5.5, DeRozan 4.1,
Lou Williams 3.9, Kevin Martin 3.6, Reggie Miller 3.4, Trae 3.4, Butler 3.2, Dončić 3.0, Dinwiddie 2.7,
Wade 2.7. Harden's 726 fouled threes against 143 expected is the single largest anomaly in the data.

## 5. Caveats
- 1996-2004 and 2025-26 don't name the fouled player on non-shooting fouls. FT shooters cover shooting and bonus
  fouls; the rest is imputed (`imputed` column; ~14–35% of fouls for those careers). Shot accounting needs no
  imputation.
- 2025-26 comes from the v3 converter. 1% of substitutions can't be resolved, so lineups are clean for 89% of
  period-lineups (vs ~94%), which affects bench and trouble splits only.
- Location imputation for fouled misses assumes ψ = κ·FG%. The 3PT κ (observed) and the rim κ (solved) differ,
  so mid-range foul rates depend on using the jump-shot κ there.
- The xF defender input is lineup-level (deciles of the five defenders' shooting-foul rates), not the individual
  closest defender.
- δ (value of a top-5 player per possession) is carried over from the handoff (0.0144).

## Next
1. Rerun `q3.py` (defender hesitance) and `a_eff.py` on the corrected coding.
2. A proper EP-by-state model as a cross-check on the component EPA.
3. Playoff xFoul / REAVES (the parse is done; `xfoul.py` reads `out/` only).
4. Shot-type (not just zone) xFoul using shot-detail action types for and-ones.
