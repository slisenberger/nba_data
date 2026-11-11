"""Per-player ratio (all counting fouls drawn) / (shooting + penalty fouls drawn), from seasons where the PBP names
the fouled player (2005+). cf.py uses it to impute the drawer of non-shooting, non-penalty fouls in 1996-2004,
where only FT trips reveal who was fouled."""
import pandas as pd, glob
F=pd.concat([pd.read_parquet(f,columns=['drawn_by','action','pen_before']) for f in sorted(glob.glob('/home/claude/data/out/fouls_*.parquet')) if int(f[-12:-8])>=2005])
F=F[F.drawn_by>0]
sp=F.action.isin([2,29])|(F.pen_before==1)
g=pd.DataFrame({'all':F.groupby('drawn_by').size(),'sp':F[sp].groupby('drawn_by').size()}).fillna(0)
league=g['all'].sum()/g['sp'].sum()
k=20  # shrink toward league ratio with k pseudo-shoot/pen fouls
g['r']=(g['all']+k*league)/(g['sp']+k)
out=g.reset_index().rename(columns={'drawn_by':'pid'})[['pid','r']]
out.attrs['league']=league
out.to_parquet('/home/claude/data/ratio.parquet'); print('league ratio',league,len(out))
