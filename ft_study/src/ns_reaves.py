"""Non-shooting REAVES ("off-ball" REAVES) and combined REAVES.
Every non-shooting, non-offensive counting foul a player draws on offense (reach-ins, holds, loose-ball / rebounding,
away-from-play, inbound) except fouls the defense chose (take, hack-a, late intentional: foul_epa 'skill'==0).
  value of a drawn foul  = its foul EPA (immediate: FTs in the bonus or a side-out; + Shapley bonus credit + trouble)
  expected per possession = LPM with FEs (bonus state x period, period x minute, margin bin, defense team-season) of the
                            possession's total non-shooting-foul EPA drawn by the offense; each of the 5 on-court
                            offensive players is expected to draw 1/5 of it
  nsREAVES = actual - expected (points), per 100 on-court offensive possessions; also in foul counts.
  combined = shooting REAVES + nsREAVES (per 100 possessions).
Seasons 2005-06+ only: before that the PBP doesn't name the fouled player on non-shooting fouls (imputed in cf.py).
Clean-lineup possessions only (lq == 1), for both actual and expected."""
import pandas as pd, numpy as np, glob, gc
from fe import fe_resid
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
FE=pd.read_parquet(f'{D}/foul_epa.parquet',columns=['game_id','period','clk','season','drawer','drawer_team','shoot','skill','epa','action'])
FE=FE[(FE.shoot==0)&(FE.skill==1)&(FE.drawer>0)&(FE.season>=2005)]
rows=[]; prow=[]
for y in range(2005,2026):
    P=pd.read_parquet(f'{D}/out/poss_{y}.parquet',columns=['game_id','period','off','deff','start_clk','end_clk','def_pen','margin','off_lu','lq'])
    P['season']=y; P['pi']=np.arange(len(P))
    f=FE[FE.season==y].copy()
    key=lambda g,p,t: (g.astype(np.int64)*10+p.astype(np.int64))*100+t.astype(np.int64)%100
    P['k']=key(P.game_id,P.period,P.off); f['k']=key(f.game_id,f.period,f.drawer_team)
    a=P[['k','start_clk','pi']].assign(neg=-P.start_clk).sort_values('neg'); b=f.assign(neg=-f.clk).sort_values('neg')
    m=pd.merge_asof(b,a[['neg','k','pi']],on='neg',by='k',direction='backward')
    m=m[m.pi.notna()]; m['pi']=m.pi.astype(int)
    tot=m.groupby('pi').agg(ns_epa=('epa','sum'),ns_n=('epa','size'))
    P=P.join(tot,on='pi'); P[['ns_epa','ns_n']]=P[['ns_epa','ns_n']].fillna(0)
    m['lq']=P.lq.to_numpy()[m.pi.to_numpy()]
    m['lu']=[P.off_lu.iat[i] for i in m.pi.to_numpy()]
    rows.append(m[m.lq==1][['drawer','season','epa','lu']])
    prow.append(P[['season','period','start_clk','def_pen','margin','deff','ns_epa','ns_n','off_lu','lq']])
    print(y,'possessions',len(P),'ns fouls matched',len(m),'of',len(f),flush=True)
P=pd.concat(prow,ignore_index=True); A=pd.concat(rows,ignore_index=True); del prow,rows; gc.collect()
P['f_pen']=P.def_pen*10+P.period.clip(upper=5); P['f_clk']=P.period.clip(upper=5)*100+(P.start_clk//60).astype(int)
P['f_mb']=(P.margin.clip(-24,24)//4).astype(int); P['f_dts']=P.deff.astype(np.int64)%100000*10000+P.season
fes=['f_pen','f_clk','f_mb','f_dts']
P['x_epa']=P.ns_epa-fe_resid(P,'ns_epa',fes); P['x_n']=P.ns_n-fe_resid(P,'ns_n',fes)
print('league: ns-foul EPA per possession',round(P.ns_epa.mean(),4),' ns fouls per possession',round(P.ns_n.mean(),4))
Q=P[P.lq==1]
# expected per player: 1/5 of each on-court possession's expectation
import pyarrow as pa
lu=Q.off_lu.to_numpy()
ex=pd.DataFrame({'pid':np.concatenate(lu),'season':np.repeat(Q.season.to_numpy(),[len(x) for x in lu]),
                 'x_epa':np.repeat(Q.x_epa.to_numpy()/5,[len(x) for x in lu]),'x_n':np.repeat(Q.x_n.to_numpy()/5,[len(x) for x in lu])})
E_=ex.groupby(['pid','season']).agg(poss=('x_epa','size'),x_epa=('x_epa','sum'),x_n=('x_n','sum'))
# ---- role-adjusted expectation: who among the 5 draws a non-shooting foul, by role (conditional logit).
# Role features per player-season: usage (FGA / on-court poss; FGA only, no foul-derived attempts), assists and turnovers per on-court poss
# (ball handling; handling.py), 3PA share, rim+paint share, height, weight
# (z-scored). Fitted on all non-shooting fouls with clean lineups; expected share of each possession's xNS = softmax.
R0=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','season','att','off_poss']].rename(columns={'shooter':'pid'})
XA=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['season','shooter','z','w','kind'])
XA=XA[(XA.kind!='fouled_miss')&(XA.season>=2005)]
zs=XA.assign(t3=XA.z.isin(['C3','AB3']),rim=XA.z.isin(['RA','PAINT'])).groupby(['shooter','season']).agg(t3=('t3','mean'),rim=('rim','mean'),fga=('w','size')).reset_index().rename(columns={'shooter':'pid'})
H_=pd.read_csv(f'{D}/ext/heights.csv')[['shooter','height_in','weight']].rename(columns={'shooter':'pid'})
RF=R0.merge(zs,on=['pid','season'],how='outer').merge(H_,on='pid',how='left')
HD=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'{D}/handling/handling_*.parquet'))])   # handling.py
RF=RF.merge(HD,on=['pid','season'],how='left')
RF['usage']=RF.fga/RF.off_poss;   # FGA only: no fouled misses or free throws, which would put the outcome in the control
RF['ast_rate']=RF.ast/RF.off_poss; RF['tov_rate']=RF.tov/RF.off_poss   # ball handling
feat=['usage','ast_rate','tov_rate','t3','rim','height_in','weight']
for c in feat: RF[c]=RF[c].fillna(RF[c].median())
RF[feat]=(RF[feat]-RF[feat].mean())/RF[feat].std()
RF['usage2']=RF.usage**2; feat2=feat+['usage2']
fmap={(a,b):v for a,b,v in zip(RF.pid,RF.season,RF[feat2].to_numpy())}
z0=np.zeros(len(feat2))
def fm(pid,season): return fmap.get((pid,season),z0)
ev=A[A.lu.map(len)==5]
Fm=np.array([[fm(int(p),s) for p in lu] for lu,s in zip(ev.lu,ev.season)])           # events x 5 x k
ch=np.array([list(lu).index(d) if d in list(lu) else -1 for lu,d in zip(ev.lu,ev.drawer)])
ok=ch>=0; Fm=Fm[ok]; ch=ch[ok]
from scipy.optimize import minimize
def nll(b):
    u=Fm@b; u=u-u.max(1,keepdims=True); lp=u-np.log(np.exp(u).sum(1,keepdims=True)); return -lp[np.arange(len(ch)),ch].sum()
def grad(b):
    u=Fm@b; u=u-u.max(1,keepdims=True); pr=np.exp(u); pr/=pr.sum(1,keepdims=True)
    return -(Fm[np.arange(len(ch)),ch]-(pr[:,:,None]*Fm).sum(1)).sum(0)
bh=minimize(nll,np.zeros(len(feat2)),jac=grad,method='BFGS').x
ll0=len(ch)*np.log(5); print('role model (conditional logit) coefs',dict(zip(feat2,np.round(bh,3))),' pseudo-R2',round(1-nll(bh)/ll0,3),' events',len(ch))
# role shares for every clean possession lineup
Qf=Q[Q.off_lu.map(len)==5]
L=np.array([[fm(int(p),s) for p in lu] for lu,s in zip(Qf.off_lu,Qf.season)])
u=L@bh; u=u-u.max(1,keepdims=True); sh=np.exp(u); sh/=sh.sum(1,keepdims=True)
exr=pd.DataFrame({'pid':np.concatenate(Qf.off_lu.to_numpy()),'season':np.repeat(Qf.season.to_numpy(),5),
                  'x_epa_r':(sh*Qf.x_epa.to_numpy()[:,None]).ravel(),'x_n_r':(sh*Qf.x_n.to_numpy()[:,None]).ravel()})
E_=E_.join(exr.groupby(['pid','season'])[['x_epa_r','x_n_r']].sum(),how='left')
A_=A.drop(columns=['lu']).groupby(['drawer','season']).agg(ns_epa=('epa','sum'),ns_n=('epa','size')); A_.index.names=['pid','season']
S=E_.join(A_,how='left').fillna(0).reset_index()
S['ns_reaves']=S.ns_epa-S.x_epa; S['ns_excess_fouls']=S.ns_n-S.x_n
S['ns_reaves_role']=S.ns_epa-S.x_epa_r; S['ns_reaves_role_per100poss']=100*S.ns_reaves_role/S.poss
S['ns_reaves_per100poss']=100*S.ns_reaves/S.poss; S['ns_excess_per100poss']=100*S.ns_excess_fouls/S.poss
names=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'{D}/out/names_*.parquet'))]).drop_duplicates('pid').set_index('pid').name
S['name']=S.pid.map(names)
# combine with shooting REAVES (per 100 poss on its own possession base)
R=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','season','reaves','off_poss','reaves_size']].rename(columns={'shooter':'pid'})
S=S.merge(R,on=['pid','season'],how='left')
S['shoot_reaves_per100poss']=100*S.reaves/S.off_poss
S['combined_per100poss']=S.shoot_reaves_per100poss.fillna(0)+S.ns_reaves_per100poss
S['combined_role_per100poss']=S.shoot_reaves_per100poss.fillna(0)+S.ns_reaves_role_per100poss
S.round(4).to_csv(f'{OUT}/ns_reaves_seasons.csv',index=False)
C=S.groupby('pid').agg(name=('name','first'),first=('season','min'),last=('season','max'),poss=('poss','sum'),ns_n=('ns_n','sum'),x_n=('x_n','sum'),
                        ns_epa=('ns_epa','sum'),x_epa=('x_epa','sum'),ns_reaves=('ns_reaves','sum'),ns_reaves_role=('ns_reaves_role','sum'),x_n_r=('x_n_r','sum'),reaves=('reaves','sum'),off_poss=('off_poss','sum'))
C=C[C.poss>=15000].copy()
C['ns_ratio']=C.ns_n/C.x_n; C['ns_reaves_per100poss']=100*C.ns_reaves/C.poss
C['ns_role_ratio']=C.ns_n/C.x_n_r; C['ns_reaves_role_per100poss']=100*C.ns_reaves_role/C.poss; C['shoot_reaves_per100poss']=100*C.reaves/C.off_poss; C['combined_role_per100poss']=C.shoot_reaves_per100poss+C.ns_reaves_role_per100poss; C['combined_per100poss']=C.shoot_reaves_per100poss+C.ns_reaves_per100poss
C.reset_index().round(4).to_csv(f'{OUT}/ns_reaves_career.csv',index=False)
pd.set_option('display.width',250)
c=['name','first','last','poss','ns_n','x_n','ns_ratio','ns_reaves_per100poss','shoot_reaves_per100poss','combined_per100poss']
print('CAREER (2005-06+), non-shooting REAVES per 100 poss'); print(C.sort_values('ns_reaves_per100poss',ascending=False)[c].head(20).round(2).to_string(index=False))
print('bottom'); print(C.sort_values('ns_reaves_per100poss')[c].head(8).round(2).to_string(index=False))
print('COMBINED'); print(C.sort_values('combined_per100poss',ascending=False)[c].head(20).round(2).to_string(index=False))

c2=['name','first','last','ns_ratio','ns_role_ratio','ns_reaves_per100poss','ns_reaves_role_per100poss','combined_role_per100poss']
print('ROLE-ADJUSTED non-shooting REAVES'); print(C.sort_values('ns_reaves_role_per100poss',ascending=False)[c2].head(20).round(2).to_string(index=False))
print(C[C.name.isin(['Trae Young','Chris Paul','Nikola Jokic','Joel Embiid','James Harden','DeMarcus Cousins','Rudy Gobert'])][c2].round(2).to_string(index=False))
