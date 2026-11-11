"""Possessions a fouler loses after his nth personal foul, by quarter AND time left in the quarter (3-min bins).
rem_poss (cf.py: the fouler's remaining on-court possessions, both ends) ~ dummies(n x clock bin) with player-season
FE, separately per quarter; the clock-bin main effect is absorbed by bin FE, so level(n, bin) is relative to a 1st
foul committed in the same bin. Marginal loss of the nth foul at bin b = level(n,b) - level(n-1,b)."""
import pandas as pd, numpy as np, glob
from fe import feols
F=pd.concat([pd.read_parquet(f,columns=['game_id','period','clk','fouler','fouler_pf','rem_poss','season']) for f in sorted(glob.glob('/home/claude/data/cf/cf_fouls_*.parquet'))],ignore_index=True)
F=F[(F.fouler>0)&(F.period<=4)&(F.fouler_pf.between(1,6))&F.rem_poss.notna()]
F['cb']=np.minimum(F.clk//180,3).astype(int)   # 0: last 3 min ... 3: first 3 min
F['ps']=F.fouler*100+F.season%100
out=[]
for p in [1,2,3,4]:
    d=F[F.period==p].copy()
    cells=[(n,b) for n in range(2,7) for b in range(4) if ((d.fouler_pf==n)&(d.cb==b)).sum()>=300]
    for n,b in cells: d[f'n{n}b{b}']=((d.fouler_pf==n)&(d.cb==b)).astype(float)
    o=feols(d,['rem_poss'],[f'n{n}b{b}' for n,b in cells],['ps','cb'],'game_id')
    lv={c:v for c,v in zip(cells,o['rem_poss'][0])}; se={c:v for c,v in zip(cells,o['rem_poss'][1])}
    for n,b in cells:
        prev=lv.get((n-1,b),0.0 if n==2 else np.nan)
        out.append(dict(period=p,nth_foul=n,clock_bin=b,level=lv[(n,b)],marginal_poss_lost=-(lv[(n,b)]-prev),se_level=se[(n,b)],count=int(((d.fouler_pf==n)&(d.cb==b)).sum())))
R=pd.DataFrame(out); R.to_csv('/home/user/nba_data/ft_study/results/res_minutes_clock.csv',index=False)
print(R.pivot_table(index=['period','nth_foul'],columns='clock_bin',values='marginal_poss_lost').round(1).to_string())
