"""Is the playoff whistle compression about REAVES (fouls beyond expected) or about shot mix (expected foul rate)?
Regular-season attempts vs non-playoff teams (base) give each player-season: n attempts, f fouls drawn, xf (xFoul expected).
  er  = xf/n          expected foul rate (shot location and context)
  foe = (f+k)/(xf+k)  fouls / expected, shrunk (k=25 fouls), i.e. REAVES as a ratio
Prediction for a target segment = target attempts x EB-shrunk base rate x league factor (as po_whistle.py).
Poisson regression of target fouls on z(log er) and z(log foe) with log(prediction) as offset, for the playoffs and the
placebo (regular-season games vs playoff teams). The playoff-minus-placebo coefficient says which trait loses calls."""
import pandas as pd, numpy as np
from scipy.optimize import minimize
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
X=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['game_id','season','shooter','w','xf','fouled'])
X=X[X.season<=2024]; X['xfw']=X.xf*X.w; X['fw']=X.fouled*X.w
XG=X.groupby(['season','game_id','shooter']).agg(n=('w','sum'),xf=('xfw','sum'),f=('fw','sum')).reset_index(); del X
rows=[]
for y in range(1996,2025):
    p=pd.read_parquet(f'{D}/out_po/att_{y}.parquet',columns=['shooter','kind','off']); po_teams=set(p.off.unique())
    r=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','shooter','deff']).drop_duplicates(['game_id','shooter'])
    g=XG[XG.season==y].merge(r,on=['game_id','shooter'],how='inner'); g['seg']=np.where(g.deff.isin(po_teams),'rs_po','rs_np')
    s=g.groupby(['shooter','seg'])[['n','xf','f']].sum().unstack('seg'); s.columns=[a+'_'+b for a,b in s.columns]; s=s.fillna(0)
    p=p[p.kind.isin(['fga','and1','fouled_miss'])&(p.shooter>0)]
    pp=p.assign(f=p.kind.isin(['and1','fouled_miss'])).groupby('shooter').agg(n_po=('f','size'),f_po=('f','sum'))
    s=s.join(pp,how='left').fillna(0).reset_index(); s['season']=y; rows.append(s)
S=pd.concat(rows); S=S[S.n_rs_np>=100].copy()
m=S.f_rs_np.sum()/S.n_rs_np.sum(); r_=S.f_rs_np/S.n_rs_np; noise=m*(1-m)/S.n_rs_np
tau2=np.average((r_-m)**2,weights=S.n_rs_np)-np.average(noise,weights=S.n_rs_np); S['base']=m+tau2/(tau2+noise)*(r_-m)
S['er']=S.xf_rs_np/S.n_rs_np; S['foe']=(S.f_rs_np+25)/(S.xf_rs_np+25)
S['z_er']=(np.log(S.er)-np.log(S.er).mean())/np.log(S.er).std(); S['z_foe']=(np.log(S.foe)-np.log(S.foe).mean())/np.log(S.foe).std()
print('player-seasons',len(S),' corr(z_er,z_foe)',round(S.z_er.corr(S.z_foe),3))
res={}
for tgt in ['rs_po','po']:
    Y=S[S['n_'+tgt]>0]; fac=Y['f_'+tgt].sum()/(Y.base*Y['n_'+tgt]).sum()
    off=np.log(fac*Y.base*Y['n_'+tgt]).to_numpy(); yv=Y['f_'+tgt].to_numpy(); Xm=np.column_stack([np.ones(len(Y)),Y.z_er,Y.z_foe])
    nll=lambda b: (np.exp(off+Xm@b)-yv*(off+Xm@b)).sum()
    b=minimize(nll,np.zeros(3),jac=lambda b: Xm.T@(np.exp(off+Xm@b)-yv),method='BFGS').x
    mu=np.exp(off+Xm@b); V=np.linalg.inv((Xm*mu[:,None]).T@Xm)
    # cluster-robust by player (sandwich)
    sc=Xm*(yv-mu)[:,None]; cl=pd.DataFrame(sc).groupby(Y.shooter.to_numpy()).sum().to_numpy(); Vr=V@(cl.T@cl)@V
    res[tgt]=(b,np.sqrt(np.diag(Vr)))
    print(tgt,'n',len(Y),' coef per SD: shot-mix(er)',round(b[1],4),'+-',round(np.sqrt(Vr[1,1]),4),'  REAVES(foe)',round(b[2],4),'+-',round(np.sqrt(Vr[2,2]),4))
for i,lab in [(1,'shot-mix'),(2,'REAVES')]:
    d=res['po'][0][i]-res['rs_po'][0][i]; se=np.hypot(res['po'][1][i],res['rs_po'][1][i]); print('playoffs minus placebo,',lab,round(d,4),'+-',round(se,4))
# quintiles of foe within the playoffs, actual / predicted, holding er via the regression offset only
Y=S[S.n_po>0].copy(); fac=Y.f_po.sum()/(Y.base*Y.n_po).sum(); Y['pred']=fac*Y.base*Y.n_po
Z=S[S.n_rs_po>0].copy(); fz=Z.f_rs_po.sum()/(Z.base*Z.n_rs_po).sum(); Z['pred']=fz*Z.base*Z.n_rs_po
for lab,col in [('REAVES ratio (foe)','foe'),('expected rate (er)','er')]:
    print('quintiles of',lab)
    for q in range(5):
        lo,hi=S[col].quantile(q/5),S[col].quantile((q+1)/5)
        a=Y[(Y[col]>=lo)&(Y[col]<=hi)]; b=Z[(Z[col]>=lo)&(Z[col]<=hi)]
        print(f'  q{q+1} {col} {lo:.3f}-{hi:.3f}: playoffs {a.f_po.sum()/a.pred.sum():.3f} +-{np.sqrt(a.pred.sum())/a.pred.sum():.3f}   placebo {b.f_rs_po.sum()/b.pred.sum():.3f}')
