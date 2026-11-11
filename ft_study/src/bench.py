"""Bench (and on-floor) bonus value of each player's drawn fouls: bonus possessions his fouls created for his team
(cf.py joint removal), split by whether he was on the floor, valued at beta (betas.json; corrected bonus test)."""
import pandas as pd, numpy as np, glob, json
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
B=json.load(open(f'{D}/betas.json'))
names=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'{D}/out/names_*.parquet'))]).drop_duplicates('pid').set_index('pid').name
S=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'{D}/cf/cf_players_*.parquet'))])
gp=pd.concat([pd.read_parquet(f,columns=['game_id','off_lu','def_lu']) for f in sorted(glob.glob(f'{D}/out/poss_*.parquet'))[:0]]) if False else None
A=pd.concat([pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','shooter']) for y in range(1996,2026)])
g=A[A.shooter>0].groupby('shooter').game_id.nunique()
C=S.groupby('pid').agg(first=('season','min'),last=('season','max'),off_poss=('off_poss','sum'),drawn=('drawn_counting','sum'),flips_on=('flips_on','sum'),flips_off=('flips_off','sum'))
C['gp']=g; C['name']=C.index.map(names); C=C[C.off_poss>=20000]
C['drawn_per100']=100*C.drawn/C.off_poss
for k in ['low','mid','high']: C[f'bench_pts_{k}']=C.flips_off*B[f'beta_{k}']
C['bench_pts_per_g']=C.bench_pts_mid/C.gp; C['on_pts_mid']=C.flips_on*B['beta_mid']
C['bench_share']=C.flips_off/(C.flips_on+C.flips_off)
C.reset_index().round(4).to_csv(f'{OUT}/bench_career.csv',index=False)
pd.set_option('display.width',250)
cols=['name','first','last','gp','drawn_per100','flips_on','flips_off','bench_share','bench_pts_low','bench_pts_mid','bench_pts_high','bench_pts_per_g','on_pts_mid']
print(C.sort_values('drawn_per100',ascending=False)[cols].head(15).round(3).to_string())
print(C.sort_values('bench_pts_mid',ascending=False)[cols].head(15).round(3).to_string())
