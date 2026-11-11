"""Tune the fouled-miss zone imputation on seasons with logged locations (2016-17+).
For each player-season: logged zone split of 2PT fouled misses (confusion-corrected, from xfoul_att.parquet) vs the
imputed split under variants. Score = REAVES-relevant error: a fouled miss in zone z contributes (1 - phi_z) * dv_z,
so err_p = F_p * sum_z (imp_z - log_z) * (1 - phi_z) * dv_z (pts). Variants: shrink strength m (pseudo and-ones toward
the FGA-implied mix) and a dunk-share adjustment of the rim conversion rate psi_RA (fitted on 2016+)."""
import pandas as pd, numpy as np, json
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
Z2=['RA','PAINT','MID']
X=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['season','shooter','kind','z','w','is3','made','pts','dv'])
X=X[(X.season>=2016)&(X.is3==0)]
par={int(k):v for k,v in json.load(open(f'{D}/xfoul_par.json')).items()}
KAP=json.load(open(f'{D}/xfoul_kappa.json'))
fg=X[X.kind!='fouled_miss']; fm=X[X.kind=='fouled_miss']
n=fg.groupby(['shooter','season','z']).size().unstack(fill_value=0).reindex(columns=Z2,fill_value=0)
a=fg[fg.kind=='and1'].groupby(['shooter','season','z']).size().unstack(fill_value=0).reindex(index=n.index,columns=Z2,fill_value=0)
lg=fm.groupby(['shooter','season','z']).w.sum().unstack(fill_value=0).reindex(columns=Z2,fill_value=0)
# dunk share of rim FGAs
SD=pd.concat([pd.read_parquet(f'{D}/sd/sd_{y}.parquet',columns=['game_id','evnum','zone','sd_action']).assign(season=y) for y in range(2016,2026)])
A=pd.concat([pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','evnum','shooter','kind']).assign(season=y) for y in range(2016,2026)])
r=A[A.kind.isin(['fga','and1'])].merge(SD,on=['game_id','evnum','season']); r=r[r.zone=='Restricted Area']
dunk=r.assign(d=r.sd_action.astype(str).str.contains('Dunk')).groupby(['shooter','season']).d.mean()
idx=lg.index.intersection(n.index); lg=lg.loc[idx]; n=n.loc[idx]; a=a.loc[idx]
F=lg.sum(1); dk=dunk.reindex(idx).fillna(dunk.mean()).to_numpy()
seasons=idx.get_level_values(1).to_numpy()
# league per season: g (unfouled FG%), phi, dv
g=fg[fg.kind=='fga'].groupby(['season','z']).made.mean().unstack()[Z2]
phi=pd.DataFrame({s:par[s]['phi'] for s in par}).T[Z2]
dv=X.groupby(['season','z']).dv.mean().unstack()[Z2]
arate=pd.DataFrame({s:par[s]['arate'] for s in par}).T[Z2]
G=g.loc[seasons].to_numpy(); PHI=phi.loc[seasons].to_numpy(); DV=dv.loc[seasons].to_numpy(); AR=arate.loc[seasons].to_numpy()
LOG=(lg.to_numpy()/np.maximum(F.to_numpy()[:,None],1e-9))
# fit dunk adjustment of psi_RA on logged data: psi_RA,p = a1/(a1+fm_RA)
w=a['RA'].to_numpy()+lg['RA'].to_numpy(); ok=w>=20
psi_obs=a['RA'].to_numpy()/np.maximum(w,1e-9)
b1,b0=np.polyfit(dk[ok],psi_obs[ok],1,w=w[ok]); mdk=np.average(dk[ok],weights=w[ok])
print(f'psi_RA ~ {b0:.3f} + {b1:.3f} * dunk_share  (weighted mean dunk share {mdk:.3f})')
def impute(m,dunk_adj,paint_adj=0.0):
    psi=np.c_[[KAP[z]*G[:,i] for i,z in enumerate(Z2)]].T
    if dunk_adj: psi[:,0]=psi[:,0]*(b0+b1*dk)/(b0+b1*mdk)
    e=n.to_numpy()*AR; aa=a.to_numpy().astype(float)
    sh=(aa+m*e/np.maximum(e.sum(1,keepdims=True),1e-9))/(aa.sum(1,keepdims=True)+m) if m<1e8 else e/np.maximum(e.sum(1,keepdims=True),1e-9)
    wv=sh*(1-psi)/psi; return wv/np.maximum(wv.sum(1,keepdims=True),1e-12)
rows=[]
for dunk_adj in (0,1):
    for m in [0,2,5,10,20,40,80,1e9]:
        IMP=impute(m,dunk_adj)
        err=F.to_numpy()*((IMP-LOG)*(1-PHI)*DV).sum(1)
        car=pd.Series(err,index=idx).groupby(level=0).sum()
        shares=np.abs(IMP-LOG).sum(1)/2
        rows.append(dict(dunk_adj=dunk_adj,m=('FGA mix only' if m>1e8 else m),season_rmse=np.sqrt(np.mean(err**2)),season_mae=np.mean(np.abs(err)),
                         career_mae=car.abs().mean(),career_max=car.abs().max(),share_err=np.average(shares,weights=F),gobert=car.get(203497,np.nan),bias=err.mean()))
R=pd.DataFrame(rows); R.round(3).to_csv(f'{OUT}/impute_tuning.csv',index=False); print(R.round(3).to_string())
json.dump(dict(b0=b0,b1=b1,mdk=mdk),open(f'{D}/dunk_psi.json','w'))
# ---- direct split correction: log(paint/RA) of logged vs imputed, regressed on dunk share (and rim share of FGA)
IMP=impute(20,0)
rimsh=(n['RA']/n.sum(axis=1)).to_numpy()
okk=(lg['RA'].to_numpy()+lg['PAINT'].to_numpy())>=15
lr=np.log((LOG[:,1]+.02)/(LOG[:,0]+.02))-np.log((IMP[:,1]+.02)/(IMP[:,0]+.02))
Xr=np.c_[np.ones(okk.sum()),dk[okk]-mdk,rimsh[okk]-rimsh[okk].mean()]
coef=np.linalg.lstsq(Xr*np.sqrt(F.to_numpy()[okk])[:,None],lr[okk]*np.sqrt(F.to_numpy()[okk]),rcond=None)[0]
print('log(paint/RA) correction: const %.3f, dunk %.3f, rim share %.3f'%tuple(coef))
rim_mean=rimsh[okk].mean()
def impute2(m,c0,c1,c2):
    I=impute(m,0).copy(); f=np.exp(c0+c1*(dk-mdk)+c2*(rimsh-rim_mean)); I[:,1]*=f; return I/I.sum(1,keepdims=True)
rows=[]
for lab,cc in [('none',(0,0,0)),('const',(coef[0],0,0)),('dunk',(coef[0],coef[1],0)),('dunk+rim',tuple(coef))]:
    for m in (20,40):
        I=impute2(m,*cc); err=F.to_numpy()*((I-LOG)*(1-PHI)*DV).sum(1); car=pd.Series(err,index=idx).groupby(level=0).sum()
        rows.append(dict(split_corr=lab,m=m,season_rmse=np.sqrt(np.mean(err**2)),career_mae=car.abs().mean(),career_max=car.abs().max(),gobert=car.get(203497),bias=err.mean()))
R2=pd.DataFrame(rows); print(R2.round(3).to_string()); R2.round(3).to_csv(f'{OUT}/impute_tuning_split.csv',index=False)
json.dump(dict(b0=b0,b1=b1,mdk=mdk,c=list(coef),rim_mean=rim_mean),open(f'{D}/dunk_psi.json','w'))
