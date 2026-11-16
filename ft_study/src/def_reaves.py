"""Defensive REAVES: shooting fouls COMMITTED above expected, accumulated to the defender.
For each opponent shooting attempt with a clean defensive lineup: expected foul = xF (xfoul.py, same context model,
fractional rows summed per attempt); each of the 5 on-court defenders is charged 1/5 of it. Actual = 1 for the defender
named as the fouler on a fouled attempt. Value in points = (fouled - xF) x dv (league value of a fouled vs unfouled
attempt), charged the same way. Positive = commits more shooting fouls than an average defender on the floor for the
same shots. Also raw counts of all personal fouls committed (fouls tables)."""
import pandas as pd, numpy as np, glob, gc
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
X=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['game_id','season','shooter','kind','period','clk','w','xf','dv','fouled','fouler','z'])
X['c10']=(X.clk.astype(float)*10).round().astype(np.int64)
X['xfw']=X.xf*X.w; X['dvw']=X.dv*X.w; X['rimw']=X.w*X.z.isin(['RA','PAINT'])
A_=X.groupby(['game_id','period','c10','shooter','kind']).agg(xf=('xfw','sum'),dvx=('dvw','sum'),fouled=('fouled','max'),fouler=('fouler','first'),season=('season','first'),rim=('rimw','sum')).reset_index()
del X; gc.collect()
rows=[]
for y in range(1996,2026):
    a=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','period','clk','shooter','kind','def_lu'])
    a=a[a.kind.isin(['fga','and1','fouled_miss'])&(a.shooter>0)]
    a['c10']=(a.clk.astype(float)*10).round().astype(np.int64)
    a=a.drop_duplicates(['game_id','period','c10','shooter','kind'])
    m=a.merge(A_[A_.season==y],on=['game_id','period','c10','shooter','kind'],how='inner')
    m=m[m.def_lu.map(len)==5]
    m['season']=y; rows.append(m[['season','def_lu','xf','dvx','fouled','fouler','rim']])
    print(y,'attempts',len(m),flush=True)
M=pd.concat(rows,ignore_index=True); del rows; gc.collect()
# ---- who contests: conditional logit for which of the 5 defenders commits a shooting foul, by size and shot location.
# features per defender: height, weight (z-scored), each x (1 - rim) and x rim (rim = share of the attempt at rim/paint)
H=pd.read_csv(f'{D}/ext/heights.csv')[['shooter','height_in','weight']].rename(columns={'shooter':'pid'}).dropna()
hz=dict(zip(H.pid,(H.height_in-H.height_in.mean())/H.height_in.std())); wz=dict(zip(H.pid,(H.weight-H.weight.mean())/H.weight.std()))
LU=np.stack(M.def_lu.to_numpy()); hh=np.vectorize(lambda p: hz.get(p,0.0))(LU); ww=np.vectorize(lambda p: wz.get(p,0.0))(LU)
rim=M.rim.to_numpy()[:,None]
Fm=np.stack([hh*(1-rim),ww*(1-rim),hh*rim,ww*rim],axis=2)
ev=(M.fouled.to_numpy()==1)
ch=np.array([list(l).index(f) if f in l else -1 for l,f in zip(LU[ev],M.fouler.to_numpy()[ev])]); ok=ch>=0
Fe=Fm[ev][ok]; ce=ch[ok]
from scipy.optimize import minimize
def nll(b):
    u=Fe@b; u=u-u.max(1,keepdims=True); return -(u[np.arange(len(ce)),ce]-np.log(np.exp(u).sum(1))).sum()
def grad(b):
    u=Fe@b; u=u-u.max(1,keepdims=True); pr=np.exp(u); pr/=pr.sum(1,keepdims=True)
    return -(Fe[np.arange(len(ce)),ce]-(pr[:,:,None]*Fe).sum(1)).sum(0)
bh=minimize(nll,np.zeros(4),jac=grad,method='BFGS').x
print('contest model coefs [height off-rim, weight off-rim, height rim, weight rim]',bh.round(3),' pseudo-R2',round(1-nll(bh)/(len(ce)*np.log(5)),3),' fouls',len(ce))
u=Fm@bh; u=u-u.max(1,keepdims=True); sh=np.exp(u); sh/=sh.sum(1,keepdims=True)
xf=M.xf.to_numpy()[:,None]; xv=(M.xf*M.dvx).to_numpy()[:,None]
E1=pd.DataFrame({'pid':LU.ravel(),'season':np.repeat(M.season.to_numpy(),5),'xf':(xf/5*np.ones((1,5))).ravel(),'xv':(xv/5*np.ones((1,5))).ravel(),
                 'xf_r':(sh*xf).ravel(),'xv_r':(sh*xv).ravel(),'att':np.full(LU.size,0.2)})
e=E1.groupby(['pid','season']).sum()
F_=M[ev].iloc[np.nonzero(ok)[0]]
act=F_.groupby(['fouler','season']).agg(fouls=('fouled','size'),fv=('dvx','sum')).rename_axis(['pid','season'])
S=e.join(act,how='left').fillna(0).reset_index()

S['excess']=S.fouls-S.xf; S['dreaves']=S.fv-S.xv          # points given away above expected (1/5 split)
S['excess_r']=S.fouls-S.xf_r; S['dreaves_r']=S.fv-S.xv_r   # contest-adjusted: split by size x shot location
# defensive possessions on floor
dp=[]
for y in range(1996,2026):
    p=pd.read_parquet(f'{D}/out/poss_{y}.parquet',columns=['def_lu','lq']); p=p[p.lq==1]
    v=np.concatenate(p.def_lu.to_numpy()); u,c=np.unique(v,return_counts=True); dp.append(pd.DataFrame({'pid':u,'season':y,'def_poss':c}))
S=S.merge(pd.concat(dp),on=['pid','season'],how='left')
# all personal fouls committed (raw), any type
pf=pd.concat([pd.read_parquet(f'{D}/out/fouls_{y}.parquet',columns=['fouler','action']).assign(season=y) for y in range(1996,2026)])
pfc=pf.groupby(['fouler','season']).size().rename('counting_fouls').reset_index().rename(columns={'fouler':'pid'})
S=S.merge(pfc,on=['pid','season'],how='left').fillna({'counting_fouls':0})
names=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'{D}/out/names_*.parquet'))]).drop_duplicates('pid').set_index('pid').name
S['name']=S.pid.map(names)
S['ratio']=S.fouls/S.xf; S['ratio_r']=S.fouls/S.xf_r; S['dreaves_r_per100poss']=100*S.dreaves_r/S.def_poss; S['excess_per100att']=100*S.excess/S.att; S['dreaves_per100poss']=100*S.dreaves/S.def_poss
S.round(4).to_csv(f'{OUT}/def_reaves_seasons.csv',index=False)
C=S.groupby('pid').agg(name=('name','first'),first=('season','min'),last=('season','max'),att=('att','sum'),def_poss=('def_poss','sum'),fouls=('fouls','sum'),xf=('xf','sum'),dreaves=('dreaves','sum'),dreaves_r=('dreaves_r','sum'),xf_r=('xf_r','sum'),counting_fouls=('counting_fouls','sum')).reset_index()
C['ratio_r']=C.fouls/C.xf_r; C['dreaves_r_per100poss']=100*C.dreaves_r/C.def_poss; C['excess_r']=C.fouls-C.xf_r; C['excess']=C.fouls-C.xf; C['ratio']=C.fouls/C.xf; C['excess_per100att']=100*C.excess/C.att; C['dreaves_per100poss']=100*C.dreaves/C.def_poss
C=C[C.def_poss>=20000]
C.round(4).to_csv(f'{OUT}/def_reaves_career.csv',index=False)
pd.set_option('display.width',250)
c=['name','first','last','def_poss','fouls','xf','ratio','excess','dreaves','dreaves_per100poss']
print('MOST SHOOTING FOULS COMMITTED (raw)'); print(C.sort_values('fouls',ascending=False)[c].head(15).round(2).to_string(index=False))
print('MOST ABOVE EXPECTED (total pts)'); print(C.sort_values('dreaves',ascending=False)[c].head(20).round(2).to_string(index=False))
print('PER 100 DEF POSS'); print(C.sort_values('dreaves_per100poss',ascending=False)[c].head(20).round(2).to_string(index=False))
print('FEWEST (per 100 def poss)'); print(C.sort_values('dreaves_per100poss')[c].head(10).round(2).to_string(index=False))

c2=['name','first','last','def_poss','fouls','xf_r','ratio_r','excess_r','dreaves_r','dreaves_r_per100poss']
print('CONTEST-ADJUSTED: most fouls above expected (total pts)'); print(C.sort_values('dreaves_r',ascending=False)[c2].head(20).round(2).to_string(index=False))
print('CONTEST-ADJUSTED per 100 def poss'); print(C.sort_values('dreaves_r_per100poss',ascending=False)[c2].head(20).round(2).to_string(index=False))
print('CONTEST-ADJUSTED fewest'); print(C.sort_values('dreaves_r_per100poss')[c2].head(15).round(2).to_string(index=False))
