"""Defensive REAVES: shooting fouls COMMITTED above expected, accumulated to the defender.
For each opponent shooting attempt with a clean defensive lineup: expected foul = xF (xfoul.py, same context model,
fractional rows summed per attempt); it is split among the 5 on-court defenders three ways:
1/5 each; by size (height, weight x rim/non-rim); by size + role (size, usage, assists, own 3PA and rim share, each x shot zone),
the last two from conditional logits for which defender commits the foul. Actual = 1 for the defender
named as the fouler on a fouled attempt. Value in points = (fouled - xF) x dv (league value of a fouled vs unfouled
attempt), charged the same way. Positive = commits more shooting fouls than an average defender on the floor for the
same shots. Also raw counts of all personal fouls committed (fouls tables)."""
import pandas as pd, numpy as np, glob, gc
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
X=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['game_id','season','shooter','kind','period','clk','w','xf','dv','fouled','fouler','z'])
X['c10']=(X.clk.astype(float)*10).round().astype(np.int64)
X['xfw']=X.xf*X.w; X['dvw']=X.dv*X.w; X['rimw']=X.w*X.z.isin(['RA','PAINT'])
for k,zz in {'zRA':['RA'],'zPT':['PAINT'],'zMD':['MID'],'z3':['C3','AB3']}.items(): X[k]=X.w*X.z.isin(zz)
A_=X.groupby(['game_id','period','c10','shooter','kind']).agg(xf=('xfw','sum'),dvx=('dvw','sum'),fouled=('fouled','max'),fouler=('fouler','first'),season=('season','first'),rim=('rimw','sum'),zRA=('zRA','sum'),zPT=('zPT','sum'),zMD=('zMD','sum'),z3=('z3','sum')).reset_index()
del X; gc.collect()
rows=[]
for y in range(1996,2026):
    a=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','period','clk','shooter','kind','def_lu'])
    a=a[a.kind.isin(['fga','and1','fouled_miss'])&(a.shooter>0)]
    a['c10']=(a.clk.astype(float)*10).round().astype(np.int64)
    a=a.drop_duplicates(['game_id','period','c10','shooter','kind'])
    m=a.merge(A_[A_.season==y],on=['game_id','period','c10','shooter','kind'],how='inner')
    m=m[m.def_lu.map(len)==5]
    m['season']=y; rows.append(m[['season','def_lu','xf','dvx','fouled','fouler','rim','zRA','zPT','zMD','z3']])
    print(y,'attempts',len(m),flush=True)
M=pd.concat(rows,ignore_index=True); del rows; gc.collect()
# ---- who commits the foul among the 5: conditional logits (softmax over the defensive lineup).
#  size model:      height, weight, each x (off-rim, rim/paint share of the attempt)
#  size+role model: per defender-season z-scored height, weight, usage (FGA / off poss), usage^2, assists / off poss,
#                   3PA share and rim+paint share of his own shots (position proxies from his offense), each x the attempt's
#                   zone shares (restricted area, paint, mid, three). No blocks/steals/fouls in the features (they'd be the outcome).
from scipy.optimize import minimize
H=pd.read_csv(f'{D}/ext/heights.csv')[['shooter','height_in','weight']].rename(columns={'shooter':'pid'}).dropna()
R0=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','season','off_poss']].rename(columns={'shooter':'pid'})
XA=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['season','shooter','z','kind']); XA=XA[XA.kind!='fouled_miss']
zs=XA.assign(t3=XA.z.isin(['C3','AB3']),rim=XA.z.isin(['RA','PAINT'])).groupby(['shooter','season']).agg(t3=('t3','mean'),rimsh=('rim','mean'),fga=('z','size')).reset_index().rename(columns={'shooter':'pid'})
del XA; gc.collect()
HD=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'{D}/handling/handling_*.parquet'))])
RF=R0.merge(zs,on=['pid','season'],how='outer').merge(H,on='pid',how='left').merge(HD,on=['pid','season'],how='left')
RF['usage']=RF.fga/RF.off_poss; RF['ast_rate']=RF.ast/RF.off_poss
RF.loc[RF.off_poss.fillna(0)<200,['usage','ast_rate','t3','rimsh']]=np.nan     # too little offense to read a role
rf=['height_in','weight','usage','ast_rate','t3','rimsh']
for c in rf: RF[c]=(RF[c]-RF[c].mean())/RF[c].std()
RF[rf]=RF[rf].fillna(0); RF['usage2']=RF.usage**2; rf2=rf+['usage2']
fmap={(a,b):v for a,b,v in zip(RF.pid,RF.season,RF[rf2].to_numpy(np.float32))}; z0=np.zeros(len(rf2),np.float32)
hz=dict(zip(H.pid,(H.height_in-H.height_in.mean())/H.height_in.std())); wz=dict(zip(H.pid,(H.weight-H.weight.mean())/H.weight.std()))
LU=np.stack(M.def_lu.to_numpy()); SEA=M.season.to_numpy()
def feats(idx,kind):
    lu=LU[idx]
    if kind=='size':
        hh=np.vectorize(lambda p: hz.get(p,0.0))(lu); ww=np.vectorize(lambda p: wz.get(p,0.0))(lu); rim=M.rim.to_numpy()[idx,None]
        return np.stack([hh*(1-rim),ww*(1-rim),hh*rim,ww*rim],axis=2).astype(np.float32)
    sea=SEA[idx]; P=np.array([[fmap.get((p,y),z0) for p in l] for l,y in zip(lu,sea)],np.float32)   # n x 5 x k
    Z=M[['zRA','zPT','zMD','z3']].to_numpy(np.float32)[idx]; Z=Z/np.maximum(Z.sum(1,keepdims=True),1e-6)
    return (P[:,:,None,:]*Z[:,None,:,None]).reshape(len(idx),5,-1)
ev=np.nonzero(M.fouled.to_numpy()==1)[0]
chf=np.array([list(l).index(f) if f in l else -1 for l,f in zip(LU[ev],M.fouler.to_numpy()[ev])]); okm=chf>=0; evk=ev[okm]; ce=chf[okm]
def fit(kind):
    Fe=feats(evk,kind)
    def nll(b):
        u=Fe@b; u=u-u.max(1,keepdims=True); return -(u[np.arange(len(ce)),ce]-np.log(np.exp(u).sum(1))).sum()
    def grad(b):
        u=Fe@b; u=u-u.max(1,keepdims=True); pr=np.exp(u); pr/=pr.sum(1,keepdims=True)
        return -(Fe[np.arange(len(ce)),ce]-(pr[:,:,None]*Fe).sum(1)).sum(0)
    b=minimize(nll,np.zeros(Fe.shape[2]),jac=grad,method='L-BFGS-B').x
    print(kind,'contest model pseudo-R2',round(1-nll(b)/(len(ce)*np.log(5)),4),' fouls',len(ce),flush=True)
    sh=np.empty((len(M),5),np.float32)
    for k in range(0,len(M),400000):
        idx=np.arange(k,min(k+400000,len(M))); u=feats(idx,kind)@b; u-=u.max(1,keepdims=True); e_=np.exp(u); sh[idx]=e_/e_.sum(1,keepdims=True)
    return b,sh
bS,shS=fit('size'); print('size coefs [h off-rim, w off-rim, h rim, w rim]',bS.round(3))
bR,sh=fit('role')
print('size+role coefs (rows: feature, cols: RA, paint, mid, three)'); print(pd.DataFrame(bR.reshape(4,len(rf2)).T,index=rf2,columns=['RA','PAINT','MID','3']).round(3))
xf=M.xf.to_numpy()[:,None]; xv=(M.xf*M.dvx).to_numpy()[:,None]
E1=pd.DataFrame({'pid':LU.ravel(),'season':np.repeat(M.season.to_numpy(),5),'xf':(xf/5*np.ones((1,5))).ravel(),'xv':(xv/5*np.ones((1,5))).ravel(),
                 'xf_r':(sh*xf).ravel(),'xv_r':(sh*xv).ravel(),'xf_s':(shS*xf).ravel(),'xv_s':(shS*xv).ravel(),'att':np.full(LU.size,0.2)})
e=E1.groupby(['pid','season']).sum()
F_=M.iloc[evk]
act=F_.groupby(['fouler','season']).agg(fouls=('fouled','size'),fv=('dvx','sum')).rename_axis(['pid','season'])
S=e.join(act,how='left').fillna(0).reset_index()

S['excess']=S.fouls-S.xf; S['dreaves']=S.fv-S.xv          # points given away above expected (1/5 split)
S['excess_r']=S.fouls-S.xf_r; S['dreaves_r']=S.fv-S.xv_r   # size+role adjusted (by shot zone)
S['dreaves_s']=S.fv-S.xv_s   # size only
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
C=S.groupby('pid').agg(name=('name','first'),first=('season','min'),last=('season','max'),att=('att','sum'),def_poss=('def_poss','sum'),fouls=('fouls','sum'),xf=('xf','sum'),dreaves=('dreaves','sum'),dreaves_r=('dreaves_r','sum'),dreaves_s=('dreaves_s','sum'),xf_r=('xf_r','sum'),counting_fouls=('counting_fouls','sum')).reset_index()
C['ratio_r']=C.fouls/C.xf_r; C['dreaves_s_per100poss']=100*C.dreaves_s/C.def_poss; C['dreaves_r_per100poss']=100*C.dreaves_r/C.def_poss; C['excess_r']=C.fouls-C.xf_r; C['excess']=C.fouls-C.xf; C['ratio']=C.fouls/C.xf; C['excess_per100att']=100*C.excess/C.att; C['dreaves_per100poss']=100*C.dreaves/C.def_poss
C=C[C.def_poss>=20000]
C.round(4).to_csv(f'{OUT}/def_reaves_career.csv',index=False)
pd.set_option('display.width',250)
c=['name','first','last','def_poss','fouls','xf','ratio','excess','dreaves','dreaves_per100poss']
print('MOST SHOOTING FOULS COMMITTED (raw)'); print(C.sort_values('fouls',ascending=False)[c].head(15).round(2).to_string(index=False))
print('MOST ABOVE EXPECTED (total pts)'); print(C.sort_values('dreaves',ascending=False)[c].head(20).round(2).to_string(index=False))
print('PER 100 DEF POSS'); print(C.sort_values('dreaves_per100poss',ascending=False)[c].head(20).round(2).to_string(index=False))
print('FEWEST (per 100 def poss)'); print(C.sort_values('dreaves_per100poss')[c].head(10).round(2).to_string(index=False))

c2=['name','first','last','def_poss','fouls','xf_r','ratio_r','excess_r','dreaves_r','dreaves_r_per100poss','dreaves_s_per100poss']
print('SIZE+ROLE ADJUSTED: most fouls above expected (total pts)'); print(C.sort_values('dreaves_r',ascending=False)[c2].head(20).round(2).to_string(index=False))
print('SIZE+ROLE ADJUSTED per 100 def poss'); print(C.sort_values('dreaves_r_per100poss',ascending=False)[c2].head(20).round(2).to_string(index=False))
print('SIZE+ROLE ADJUSTED fewest'); print(C.sort_values('dreaves_r_per100poss')[c2].head(15).round(2).to_string(index=False))
