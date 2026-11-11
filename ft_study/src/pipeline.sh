#!/bin/bash
# Full rebuild after a parser change. Logs to /home/claude/data/pipe_*.log
set -x
W=/home/claude/work; D=/home/claude/data; cd $W
seq 1996 2024 | xargs -P4 -I{} $W/runall.sh {} > $D/pipe_parse.log 2>&1
python3 $W/parse.py 2025 $D/v3/nbastats_2025.csv $D/out >> $D/pipe_parse.log 2>&1
seq 1996 2024 | xargs -P4 -I{} $W/runall.sh {} po >> $D/pipe_parse.log 2>&1
python3 ratio.py > $D/pipe_misc.log 2>&1
python3 build.py >> $D/pipe_misc.log 2>&1
seq 1996 2025 | xargs -P2 -I{} python3 cf.py {} $D/ratio.parquet $D/cf > $D/pipe_cf.log 2>&1
python3 starter.py >> $D/pipe_misc.log 2>&1
python3 q_minutes.py > $D/pipe_qmin.log 2>&1 && cp $D/res_minutes.csv /home/user/nba_data/ft_study/results/res_minutes.csv
python3 xfoul.py > $D/pipe_xfoul.log 2>&1
python3 shots.py > $D/pipe_shots.log 2>&1; PO=1 python3 shots.py > $D/pipe_shots_po.log 2>&1
python3 a_bonus_fix.py > $D/pipe_bonus.log 2>&1
python3 a_event.py > $D/pipe_event.log 2>&1
GAMMA=0 python3 foul_epa.py > $D/pipe_epa.log 2>&1
python3 reaves.py > $D/pipe_reaves.log 2>&1
python3 bench.py > $D/pipe_bench.log 2>&1
python3 grifter.py > $D/pipe_grifter.log 2>&1
echo PIPELINE DONE
