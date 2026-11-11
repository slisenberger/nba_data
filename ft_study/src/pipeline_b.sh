#!/bin/bash
# Stage B (after betas.json is updated from res_bonus_fix.csv / res_bonus_late.csv)
set -x
W=/home/claude/work; D=/home/claude/data; cd $W
GAMMA=0 python3 foul_epa.py > $D/pipe_epa.log 2>&1
python3 reaves.py > $D/pipe_reaves.log 2>&1
SUF=_imputed python3 reaves.py > $D/pipe_reaves_imp.log 2>&1
python3 validate_loc.py > $D/pipe_validate.log 2>&1
python3 bench.py > $D/pipe_bench.log 2>&1
python3 grifter.py > $D/pipe_grifter.log 2>&1
echo STAGE B DONE
