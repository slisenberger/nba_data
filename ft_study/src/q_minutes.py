import pandas as pd, numpy as np, glob
from fe import feols
F=pd.concat([pd.read_parquet(f,columns=['game_id','period','clk','fouler','fouler_pf','rem_poss','season','action']) for f in sorted(glob.glob('/home/claude/data/cf/cf_fouls_*.parquet'))],ignore_index=True)
F=F[(F.fouler>0)&(F.period<=4)&(F.fouler_pf.between(1,6))]
F['tbin']=F.period*100+(F.clk//120).astype(int)
F['ps']=F.fouler*100+F.season%100
out=[]
for p in [1,2,3,4]:
    d=F[F.period==p].copy()
    ns=[n for n in range(2,7) if (d.fouler_pf==n).sum()>500]
    for n in ns: d[f'n{n}']=(d.fouler_pf==n).astype(float)
    o=feols(d,['rem_poss'],[f'n{n}' for n in ns],['ps','tbin'],'game_id')
    c=dict(zip(ns,o['rem_poss'][0])); s=dict(zip(ns,o['rem_poss'][1]))
    prev=0
    for n in ns:
        out.append(dict(period=p,nth_foul=n,level=c[n],marginal_poss_lost=-(c[n]-prev),se_level=s[n],count=int((d.fouler_pf==n).sum()))); prev=c[n]
R=pd.DataFrame(out); R.to_csv('/home/claude/data/res_minutes.csv',index=False); print(R.round(2).to_string())
