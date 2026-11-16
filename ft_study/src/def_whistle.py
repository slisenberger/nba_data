"""Defense-side whistle: do some teams' OPPONENTS get fewer shooting fouls than their shots deserve (a bias that denies
opponents free throws rather than giving the team more)?
xF in xfoul.py includes a defending-team-season FE and the defenders' own foul-rate decile, which would absorb exactly this,
so the expectation is refit without them (zone x season, possession age x start, clock, margin, bonus, putback only).
Residual per opponent attempt r = fouled - xF_nodef. Model (attempt level, weighted, ALS):
   r = shooter-season effect + (1/5) sum of the five defenders' effects + defense team-season effect + home-defense term
Team-season effects net of shooters AND defenders are what a team-level whistle would look like. Validation: defenders split
into random halves (a team environment shows in both halves, personal defender style does not), benchmark odd/even games.
Outputs results/defwhistle_*.csv."""
import pandas as pd, numpy as np, glob, gc
from fe import fe_resid
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
cols=['game_id','season','evnum','shooter','kind','deff','fouled','w','f_zs','f_el','f_cl','f_mb','f_pen','f_pb']
X=pd.read_parquet(f'{D}/xfoul_stage1.parquet',columns=cols)
X['fouled']=X.fouled.astype(float)
X['xf_nd']=X.fouled-fe_resid(X,'fouled',['f_zs','f_el','f_cl','f_mb','f_pen','f_pb'],w='w',tol=1e-9)
X['r']=(X.fouled-X.xf_nd)
dv=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['dv']).dv.to_numpy(); assert len(dv)==len(X); X['dv']=dv
X=X.drop(columns=['f_zs','f_el','f_cl','f_mb','f_pen','f_pb']); gc.collect()
lus=[]; homes=[]
for y in range(1996,2026):
    a=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','evnum','shooter','kind','def_lu']); a=a[a.kind.isin(['fga','and1','fouled_miss'])].drop_duplicates(['game_id','evnum','shooter','kind'])
    lus.append(a)
    p=pd.read_parquet(f'{D}/out/poss_{y}.parquet',columns=['game_id','off','home_off']); homes.append(p[p.home_off==1].drop_duplicates('game_id')[['game_id','off']].rename(columns={'off':'home'}))
L=pd.concat(lus,ignore_index=True); H=pd.concat(homes)
X=X.merge(L,on=['game_id','evnum','shooter','kind'],how='inner').merge(H,on='game_id',how='left')
X=X[X.def_lu.map(len)==5].reset_index(drop=True); del L; gc.collect()
X['hd']=(X.deff==X.home).astype(float)
print('attempts with a clean defensive lineup',len(X),flush=True)
LU=np.stack(X.def_lu.to_numpy()); X=X.drop(columns=['def_lu'])
for j in range(5): X[f'd{j}']=LU[:,j]
X[['game_id','season','shooter','deff','w','r','dv','hd','xf_nd']+[f'd{j}' for j in range(5)]].to_parquet(f'{D}/defwhistle_X.parquet')
