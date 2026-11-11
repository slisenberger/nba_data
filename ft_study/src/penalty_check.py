"""Non-circular check of the penalty state (audit: the 99.9% agreement compared two counts from the same foul table).
A non-shooting, non-offensive counting foul sends the fouled team to the line iff the defense was in the penalty.
So: share of such fouls followed by an FT trip, by the parser's pen_before flag."""
import pandas as pd, glob
rows=[]
for y in range(1996,2026):
    F=pd.read_parquet(f'/home/claude/data/out/fouls_{y}.parquet',columns=['game_id','period','clk','fouler','action','pen_before','shoot'])
    A=pd.read_parquet(f'/home/claude/data/out/att_{y}.parquet',columns=['game_id','period','clk','fouler','kind'])
    t=A[A.kind=='ns_trip'][['game_id','period','clk','fouler']].drop_duplicates().assign(ft=1)
    ns=F[(F.shoot==0)&~F.action.isin([9,14,15,6])].merge(t,on=['game_id','period','clk','fouler'],how='left').fillna({'ft':0})   # excl. clear path, flagrants, away-from-play
    g=ns.groupby('pen_before').ft.mean()
    rows.append(dict(season=y,ft_after_pen=g.get(1),ft_after_nonpen=g.get(0),n_pen=int((ns.pen_before==1).sum())))
R=pd.DataFrame(rows); R.round(4).to_csv('/home/user/nba_data/ft_study/results/penalty_check.csv',index=False); print(R.round(3).to_string())
