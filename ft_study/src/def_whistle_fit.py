"""Fits for def_whistle.py (sparse ridge least squares, lsqr):
  A) r = shooter-season + defense team-season + home-defense
  B) A + (1/5) sum of the five defenders' effects
Shooter and defender effects are ridge-penalised (lam in attempt units); team-season effects are nearly unpenalised
(column scaling). Validation: odd/even games (benchmark), and for B two random halves of defenders (each fit keeps only
its half's defender columns)."""
import pandas as pd, numpy as np, scipy.sparse as sp, scipy.sparse.linalg as sla
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
X=pd.read_parquet(f'{D}/defwhistle_X.parquet'); n=len(X)
w=X.w.to_numpy(); r=X.r.to_numpy(); sw=np.sqrt(w)
ssk,ss=np.unique(X.shooter.astype(np.int64)*10000+X.season,return_inverse=True)
tsk,ts=np.unique(X.deff.astype(np.int64)*10000+X.season,return_inverse=True)
DL=X[[f'd{j}' for j in range(5)]].to_numpy(); dk,dd=np.unique(DL.ravel(),return_inverse=True); dd=dd.reshape(DL.shape)
h=X.hd.to_numpy()-np.average(X.hd,weights=w)
LAM=200.0; S_T=100.0   # team-season columns scaled by S_T -> their penalty is LAM/S_T^2 (negligible)
def design(rows, defmask=None, use_def=True):
    idx=np.nonzero(rows)[0]; m=len(idx); cols=[]; vals=[]; rr=[]
    off=0
    rr.append(np.arange(m)); cols.append(ss[idx]+off); vals.append(np.ones(m)); off+=len(ssk)
    rr.append(np.arange(m)); cols.append(ts[idx]+off); vals.append(np.full(m,S_T)); off_t=off; off+=len(tsk)
    rr.append(np.arange(m)); cols.append(np.full(m,off)); vals.append(h[idx]*S_T); off_h=off; off+=1
    off_d=off
    if use_def:
        for j in range(5):
            dj=dd[idx,j]; keep=np.ones(m,bool) if defmask is None else defmask[dj]
            rr.append(np.arange(m)[keep]); cols.append(dj[keep]+off); vals.append(np.full(keep.sum(),0.2))
        off+=len(dk)
    A=sp.csr_matrix((np.concatenate(vals),(np.concatenate(rr),np.concatenate(cols))),shape=(m,off))
    A=sp.diags(sw[idx])@A
    return A,idx,off_t,off_h,off_d
def fit(rows=None, defmask=None, use_def=True):
    rows=np.ones(n,bool) if rows is None else rows
    A,idx,ot,oh,od=design(rows,defmask,use_def)
    sol=sla.lsqr(A,sw[idx]*r[idx],damp=np.sqrt(LAM),atol=1e-10,btol=1e-10,iter_lim=3000)[0]
    b=sol[ot:ot+len(tsk)]*S_T; c=sol[oh]*S_T
    seen=np.bincount(ts[idx],minlength=len(tsk))>0
    B=pd.Series(b,index=tsk)[seen]; B=B-B.groupby(B.index%10000).transform('mean')
    return B,c
att=pd.Series(np.bincount(ts,w),index=tsk); dvm=np.average(X.dv,weights=w)
pp=[]
for y in range(1996,2026):
    p=pd.read_parquet(f'{D}/out/poss_{y}.parquet',columns=['deff']); pp.append(p.deff.value_counts().rename('poss').to_frame().assign(season=y).reset_index())
PP=pd.concat(pp); PP['ts']=PP.deff.astype(np.int64)*10000+PP.season; PP=PP.set_index('ts').poss.reindex(tsk)
conv=pd.Series(100*dvm*att.to_numpy()/PP.to_numpy(),index=tsk)
xf=X.xf_nd.clip(.001,.999).to_numpy(); noise=np.sqrt(np.average(xf*(1-xf),weights=w))
se=pd.Series(100*dvm*noise*np.sqrt(att.to_numpy())/PP.to_numpy(),index=tsk)
BA,cA=fit(use_def=False); print('A done; home-defense term (fouls per attempt)',round(cA,5),flush=True)
BB,cB=fit(use_def=True); print('B done; home-defense term',round(cB,5),flush=True)
T=pd.DataFrame({'shooters_only':BA*conv,'net_of_defenders':BB*conv,'se':se}); T['team']=T.index//10000; T['season']=T.index%10000
T.round(4).to_csv(f'{OUT}/defwhistle_team_seasons.csv')
names={1610612747:'Lakers',1610612738:'Celtics',1610612744:'Warriors',1610612748:'Heat',1610612759:'Spurs',1610612752:'Knicks',1610612745:'Rockets',1610612746:'Clippers',1610612741:'Bulls',1610612765:'Pistons',1610612755:'76ers',1610612760:'Thunder/Sonics',1610612742:'Mavericks',1610612749:'Bucks',1610612743:'Nuggets',1610612756:'Suns',1610612757:'Blazers',1610612758:'Kings',1610612762:'Jazz',1610612750:'Wolves',1610612737:'Hawks',1610612739:'Cavaliers',1610612754:'Pacers',1610612753:'Magic',1610612751:'Nets',1610612761:'Raptors',1610612764:'Wizards',1610612763:'Grizzlies',1610612766:'Hornets',1610612740:'Pelicans/Hornets'}
F=T.groupby('team').agg(seasons=('season','size'),shooters_only=('shooters_only','mean'),net=('net_of_defenders','mean'),se=('se',lambda s: np.sqrt((s**2).sum())/len(s)),neg_share=('net_of_defenders',lambda s:(s<0).mean())).reset_index()
F['name']=F.team.map(names); F['z_net']=F.net/F.se; F['z_raw']=F.shooters_only/F.se; F=F.sort_values('net')
print("franchise: opponents' shooting-foul calls vs expected, pts per 100 opponent possessions (negative = opponents get fewer)")
print(F[['name','seasons','shooters_only','z_raw','net','z_net','neg_share']].round(2).to_string(index=False)); F.round(4).to_csv(f'{OUT}/defwhistle_franchise.csv',index=False)
print('sum z^2 net',round((F.z_net**2).sum(),1),' raw',round((F.z_raw**2).sum(),1),'on',len(F),'(noise ~',len(F),')')
Lk=T[T.team==1610612747][['season','shooters_only','net_of_defenders','se']]; print('Lakers by season'); print(Lk.round(2).to_string(index=False))
VAL=[]; odd=(X.game_id%2==1).to_numpy()
for lab,ud in [('A shooters only',False),('B net of defenders',True)]:
    Bo,_=fit(odd,use_def=ud); Be,_=fit(~odd,use_def=ud); j=Bo.index.intersection(Be.index)
    VAL.append(dict(model=lab,split='games odd/even',team_season_corr=np.corrcoef(Bo[j],Be[j])[0,1],franchise_corr=np.corrcoef(Bo[j].groupby(j//10000).mean(),Be[j].groupby(j//10000).mean())[0,1]))
    print(VAL[-1],flush=True)
rng=np.random.default_rng(3)
for k in range(2):
    m=rng.random(len(dk))<0.5
    Ba,_=fit(defmask=m); Bb,_=fit(defmask=~m); j=Ba.index.intersection(Bb.index)
    VAL.append(dict(model='B',split=f'defender halves #{k+1}',team_season_corr=np.corrcoef(Ba[j],Bb[j])[0,1],franchise_corr=np.corrcoef(Ba[j].groupby(j//10000).mean(),Bb[j].groupby(j//10000).mean())[0,1]))
    print(VAL[-1],flush=True)
pd.DataFrame(VAL).round(3).to_csv(f'{OUT}/defwhistle_validation.csv',index=False)
