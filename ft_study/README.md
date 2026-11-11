# Foul-drawing study (1996-97 to 2025-26)

Play-by-play study of NBA foul drawing. `REPORT.md` has the findings; `BRIEF.md` is the original handoff.

## Pipeline
Data: github.com/shufinskiy/nba_data (`datasets/nbastats_<y>`, `nbastats_po_<y>`, `shotdetail_<y>`,
`nbastatsv3_2025`). Fetch with a sparse checkout (raw URLs are blocked by some proxies):

```
git clone --filter=blob:none --no-checkout --depth 1 https://github.com/shufinskiy/nba_data.git shuf
cd shuf && git sparse-checkout init --no-cone
printf '/datasets/nbastats_[12]*\n/datasets/shotdetail_[12]*\n/datasets/nbastatsv3_2025*\n/datasets/nbastats_po_*\n' > .git/info/sparse-checkout
git checkout HEAD
```

Scripts expect `/home/claude/data` (outputs) and `/home/claude/shufinskiy/nba_data` (the checkout); symlink them.

| step | script | output |
|---|---|---|
| parse PBP (per season; `po` for playoffs) | `runall.sh <y> [po]` → `parse.py` | `out/poss_`, `fouls_`, `att_` (one row per shooting attempt / FT trip), `refs_`, `names_` |
| 2025-26 (v3 format) | `v3conv.py` then `parse.py 2025` | same |
| shot detail | `sd.py <y>` | `sd/sd_<y>` (zone, x/y, action; joins on game + event) |
| drawer-imputation ratio | `ratio.py` | `ratio.parquet` |
| counterfactual bonus | `cf.py <y> ratio.parquet cf` | `cf/cf_fouls_`, `cf_players_` |
| possession table | `build.py` | `poss_core.parquet` |
| top-5 players | `starter.py` | `top5`, `player_team` |
| foul-trouble minutes | `q_minutes.py` | `res_minutes.csv` |
| **game-FE bonus test** | `a_bonus_gfe.py <steps>` | `res_bonus_gfe_*.csv` |
| **event study around fouls** | `a_event.py` | `res_event.csv` |
| **offense-foul placebo** | `a_offtf.py` | `res_offtf_placebo.csv` |
| **shot-attempt accounting** | `shots.py` (`PO=1` for playoffs) | `results/shots_*.csv` |
| **xFoul + REAVES** | `xfoul.py`, `reaves.py` | `xfoul_att.parquet`, `results/reaves_*.csv`, `ref_effects.csv` |
| **foul EPA** | `foul_epa.py` | `foul_epa.parquet`, `results/foul_epa_*.csv` |
| **grifters** | `grifter.py` | `results/grifter_*.csv` |

`fe.py` is the multi-way FE OLS/WLS (alternating projections, game-clustered SEs); pyfixest runs out of memory.
Handoff-era scripts (`a_bonus.py`, `a_eff.py`, `q3.py`, `delta.py`, `star.py`, `ftm.py`, `q2.py`) still run on the new
outputs; their earlier results are in `results/handoff/`.
