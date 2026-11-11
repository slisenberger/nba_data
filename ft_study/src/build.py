import pandas as pd, numpy as np, glob
cols=['game_id','period','off','deff','home_off','start_clk','end_clk','margin','def_tf','def_pen','off_tf','start_type','pts','pts_fg','pts_ft_shoot','pts_ft_bonus','pts_ft_other','fga','fgm','fg3a','fg3m','rim_a','rim_m','tov','stl','oreb','fta','d_shoot_fouls','d_nonshoot_fouls','d_nonshoot_pen','lq','season']
P=pd.concat([pd.read_parquet(f,columns=cols) for f in sorted(glob.glob('/home/claude/data/out/poss_*.parquet'))],ignore_index=True)
for c in P.columns:
    if P[c].dtype=='int64' and c not in ('game_id','off','deff'): P[c]=P[c].astype('int32')
P['start_type']=P.start_type.astype('category')
P.to_parquet('/home/claude/data/poss_core.parquet')
print(len(P), P.memory_usage(deep=True).sum()/1e9)
print(P.groupby('season').agg(n=('pts','size'),ppp=('pts','mean'),pen=('def_pen','mean')).round(3).to_string())
