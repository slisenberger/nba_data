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
w=X.w.to_numpy(); r=X.r.to_numpy()
ss=pd.factorize(X.shooter.astype(np.int64)*10000+X.season)[0]
tsk,ts=np.unique(X.deff.astype(np.int64)*10000+X.season,return_inverse=True)
dk,dd=np.unique(LU.ravel(),return_inverse=True); dd=dd.reshape(LU.shape)
def fit(mask_def=None, use_def=True, iters=60, lam=200.0):
    a=np.zeros(ss.max()+1); th=np.zeros(len(dk)); b=np.zeros(len(tsk)); c=0.0
    h=X.hd.to_numpy(); hw=h-np.average(h,weights=w)
    use=np.ones(len(dk),bool) if mask_def is None else mask_def
    for it in range(iters):
        dsum=th[dd].sum(1)/5 if use_def else 0
        a=np.bincount(ss,w*(r-dsum-b[ts]-c*h))/(np.bincount(ss,w)+lam)
        if use_def:
            res=r-a[ss]-b[ts]-c*h-dsum
            for j in range(5):
                part=res+th[dd[:,j]]/5      # residual with defender j's share added back
                num=np.bincount(dd[:,j],w*part/5,minlength=len(dk)); den=np.bincount(dd[:,j],w/25,minlength=len(dk))
                if j==0: N_=num; Dn=den
                else: N_+=num; Dn+=den
            th_new=np.where(use,N_/(Dn+lam/25),0.0); th=th_new
            dsum=th[dd].sum(1)/5
        b=np.bincount(ts,w*(r-a[ss]-dsum-c*h))/(np.bincount(ts,w)+1e-9)
        c=np.sum(w*hw*(r-a[ss]-dsum-b[ts]))/np.sum(w*hw*hw)
    B=pd.Series(b,index=tsk); B=B-B.groupby(B.index%10000).transform('mean')
    return B,c,th
# points per 100 opponent possessions conversion
att_ts=pd.Series(np.bincount(ts,w),index=tsk); dvm=np.average(X.dv,weights=w)
pp=[]
for y in range(1996,2026):
    p=pd.read_parquet(f'{D}/out/poss_{y}.parquet',columns=['deff']); pp.append(p.deff.value_counts().rename('poss').to_frame().assign(season=y).reset_index())
PP=pd.concat(pp); PP['ts']=PP.deff.astype(np.int64)*10000+PP.season; PP=PP.set_index('ts').poss
conv=100*dvm*att_ts/PP.reindex(tsk).to_numpy()          # fouls-per-attempt effect -> points per 100 opponent possessions
B0,c0,_=fit(use_def=False); B1,c1,th=fit(use_def=True)
print('home-defense term (fouls per attempt): shooters only',round(c0,5),'  + defenders',round(c1,5))
T=pd.DataFrame({'raw_pts100':B0*conv,'net_pts100':B1*conv,'att':att_ts}); T['team']=T.index//10000; T['season']=T.index%10000
noise=np.sqrt(np.average(X.xf_nd.clip(.001,.999)*(1-X.xf_nd.clip(.001,.999)),weights=w))
T['se']=100*dvm*noise/np.sqrt(T.att)*T.att/PP.reindex(T.index).to_numpy()
T.round(4).to_csv(f'{OUT}/defwhistle_team_seasons.csv')
names={1610612747:'Lakers',1610612738:'Celtics',1610612744:'Warriors',1610612748:'Heat',1610612759:'Spurs',1610612752:'Knicks',1610612745:'Rockets',1610612746:'Clippers',1610612741:'Bulls',1610612765:'Pistons',1610612755:'76ers',1610612760:'Thunder/Sonics',1610612742:'Mavericks',1610612749:'Bucks',1610612743:'Nuggets',1610612756:'Suns',1610612757:'Blazers',1610612758:'Kings',1610612762:'Jazz',1610612750:'Wolves',1610612737:'Hawks',1610612739:'Cavaliers',1610612754:'Pacers',1610612753:'Magic',1610612751:'Nets',1610612761:'Raptors',1610612764:'Wizards',1610612763:'Grizzlies',1610612766:'Hornets',1610612740:'Pelicans/Hornets'}
F=T.groupby('team').agg(seasons=('season','size'),raw=('raw_pts100','mean'),net=('net_pts100','mean'),se=('se',lambda s: np.sqrt((s**2).sum())/len(s)),neg_share=('net_pts100',lambda s:(s<0).mean())).reset_index()
F['name']=F.team.map(names); F['z_net']=F.net/F.se; F=F.sort_values('net')
print('franchise: opponents\' foul calls vs expected, points per 100 opponent possessions (negative = opponents get fewer calls)')
print(F[['name','seasons','raw','net','se','z_net','neg_share']].round(2).to_string(index=False)); F.round(4).to_csv(f'{OUT}/defwhistle_franchise.csv',index=False)
print('sum z^2',round((F.z_net**2).sum(),1),'on',len(F))
Lk=T[T.team==1610612747][['season','raw_pts100','net_pts100','se']]; print('Lakers by season'); print(Lk.round(2).to_string(index=False))
# validation: defender split (two halves of defenders get their own effects; the other half's are forced to 0 -> their style
# loads on the team term). A team environment would agree across halves; personal style would not.
rng=np.random.default_rng(3); VAL=[]
for k in range(2):
    m=rng.random(len(dk))<0.5
    Ba,_,_=fit(mask_def=m); Bb,_,_=fit(mask_def=~m); j=Ba.index.intersection(Bb.index)
    VAL.append(dict(split=f'defenders #{k+1}',team_season_corr=np.corrcoef(Ba[j],Bb[j])[0,1],franchise_corr=np.corrcoef(Ba.groupby(Ba.index//10000).mean(),Bb.groupby(Bb.index//10000).mean())[0,1]))
odd=(X.game_id%2==1).to_numpy()
def sub(mask):
    global ss,ts,dd,w,r
    s0=(ss,ts,dd,w,r,X); 
    return s0
# odd/even benchmark: refit on each half by zeroing weights
w_full=w.copy(); w=np.where(odd,w_full,0.0); Bo,_,_=fit(); w=np.where(~odd,w_full,0.0); Be,_,_=fit(); w=w_full
j=Bo.index.intersection(Be.index); VAL.append(dict(split='games odd/even (benchmark)',team_season_corr=np.corrcoef(Bo[j],Be[j])[0,1],franchise_corr=np.corrcoef(Bo.groupby(Bo.index//10000).mean(),Be.groupby(Be.index//10000).mean())[0,1]))
V=pd.DataFrame(VAL); print(V.round(3).to_string(index=False)); V.round(3).to_csv(f'{OUT}/defwhistle_validation.csv',index=False)
