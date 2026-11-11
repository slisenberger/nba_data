#!/bin/bash
# Full rebuild after a parser change. Logs to /home/claude/data/pipe_*.log
set -x
W=/home/claude/work; D=/home/claude/data; cd $W
seq 1996 2024 | xargs -P4 -I{} $W/runall.sh {} > $D/pipe_parse.log 2>&1
python3 $W/v3conv.py $D/v3/nbastatsv3_2025.csv $D/v3/nbastats_2025.csv $D/cdn25/cdnnba_2025.csv >> $D/pipe_parse.log 2>&1; python3 $W/parse.py 2025 $D/v3/nbastats_2025.csv $D/out >> $D/pipe_parse.log 2>&1
seq 1996 2024 | xargs -P4 -I{} $W/runall.sh {} po >> $D/pipe_parse.log 2>&1
python3 ratio.py > $D/pipe_misc.log 2>&1
python3 build.py >> $D/pipe_misc.log 2>&1
seq 1996 2025 | xargs -P2 -I{} python3 cf.py {} $D/ratio.parquet $D/cf > $D/pipe_cf.log 2>&1
python3 starter.py >> $D/pipe_misc.log 2>&1
python3 q_minutes.py > $D/pipe_qmin.log 2>&1 && cp $D/res_minutes.csv /home/user/nba_data/ft_study/results/res_minutes.csv
python3 q_minutes2.py > $D/pipe_qmin2.log 2>&1
python3 floc_link.py > $D/pipe_floc.log 2>&1
python3 xfoul.py > $D/pipe_xfoul.log 2>&1
FORCE_IMPUTE=1 python3 xfoul.py > $D/pipe_xfoul_imp.log 2>&1
python3 shots.py > $D/pipe_shots.log 2>&1; PO=1 python3 shots.py > $D/pipe_shots_po.log 2>&1
python3 a_bonus_fix.py > $D/pipe_bonus.log 2>&1
python3 a_bonus_late.py > $D/pipe_bonus_late.log 2>&1
seq 1996 2025 | xargs -P2 -I{} python3 bonus_credit.py {} > $D/pipe_bc.log 2>&1
python3 a_event.py > $D/pipe_event.log 2>&1
python3 penalty_check.py > $D/pipe_pencheck.log 2>&1
echo STAGE A DONE
