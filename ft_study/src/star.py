import pandas as pd, numpy as np, glob, json
B=json.load(open('/home/claude/data/betas.json'))  # beta_low, beta_mid, beta_high, delta_poss
names=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob('/home/claude/data/out/names_*.parquet'))]).drop_duplicates('pid').set_index('pid').name
S=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob('/home/claude/data/cf/cf_players_*.parquet'))])
ftm=pd.read_parquet('/home/claude/data/ftm.parquet')
S=S.merge(ftm,on=['pid','season'],how='left')
M=pd.read_csv('/home/claude/data/res_minutes.csv')
ml={(r.period,r.nth_foul):r.marginal_poss_lost for r in M.itertuples()}
def lost(p,n):
    if n<=1 or p>4: return 0.0
    if (p,n) in ml: return ml[(p,n)]
    ks=[k for k in ml if k[0]==p and k[1]<=n]; return ml[max(ks)] if ks else 0.0
F=pd.concat([pd.read_parquet(f,columns=['drawer','fouler','fouler_pf','period','season','imputed','flips_single','flips_single_on']) for f in sorted(glob.glob('/home/claude/data/cf/cf_fouls_*.parquet'))])
F=F[F.drawer>0]
F['opp_poss_lost']=[lost(p,n) if f>0 else 0 for p,n,f in zip(F.period,F.fouler_pf,F.fouler)]
T=F.groupby(['drawer','season']).agg(opp_poss_lost=('opp_poss_lost','sum'),imputed_share=('imputed','mean')).reset_index().rename(columns={'drawer':'pid'})
S=S.merge(T,on=['pid','season'],how='left').fillna({'opp_poss_lost':0,'imputed_share':0})
S['name']=S.pid.map(names)
lo,mid,hi,dl=B['beta_low'],B['beta_mid'],B['beta_high'],B['delta_poss']
S['env_on_mid']=S.flips_on*mid; S['env_off_mid']=S.flips_off*mid
S['env_off_lo']=S.flips_off*lo; S['env_off_hi']=S.flips_off*hi
S['trouble_pts']=S.opp_poss_lost*dl
S.to_parquet('/home/claude/data/star_seasons.parquet')
car=S.groupby(['pid','name']).agg(seasons=('season','nunique'),first=('season','min'),last=('season','max'),gp=('gp','sum'),off_poss=('off_poss','sum'),drawn=('drawn_counting','sum'),
    ftm=('ftm','sum'),flips_on=('flips_on','sum'),flips_off=('flips_off','sum'),opp_poss_lost=('opp_poss_lost','sum')).reset_index()
car=car[car.off_poss>=20000]
car['drawn_per100']=car.drawn/car.off_poss*100
car['bench_pen_poss_per_g']=car.flips_off/car.gp
car['bench_share']=car.flips_off/(car.flips_on+car.flips_off)
for k,b in [('lo',lo),('mid',mid),('hi',hi)]: car[f'bench_pts_{k}']=car.flips_off*b
car['bench_pts_mid_per_g']=car.flips_off*mid/car.gp
car['env_on_pts_mid']=car.flips_on*mid
car['trouble_pts']=car.opp_poss_lost*dl; car['trouble_pts_per_g']=car.trouble_pts/car.gp
car['ftm_per_g']=car.ftm/car.gp
car.to_csv('/home/claude/data/career.csv',index=False)
pd.set_option('display.width',250)
cols=['name','first','last','gp','drawn_per100','ftm_per_g','flips_on','flips_off','bench_share','bench_pen_poss_per_g','bench_pts_lo','bench_pts_mid','bench_pts_hi','bench_pts_mid_per_g','trouble_pts_per_g']
print(car.sort_values('drawn_per100',ascending=False)[cols].head(25).round(3).to_string())
print(car.sort_values('flips_off',ascending=False)[cols].head(15).round(3).to_string())
foc=['Shaquille O\'Neal','Dwight Howard','Joel Embiid']
print(S[S.name.isin(foc)][['name','season','gp','drawn_counting','ftm','flips_on','flips_off','imputed_share','env_on_mid','env_off_mid','trouble_pts']].round(2).to_string())
