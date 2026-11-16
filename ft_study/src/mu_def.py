"""Matchup-based defensive REAVES (2017-18 onward; NBA tracking matchups, shufinskiy matchups_<y>).
On-ball: each defender's shooting fouls on the players he was tracked guarding vs expected = sum over matchups of
  partial possessions x that shooter's foul-drawing rate per guarded possession that season, leaving this defender out
  and shrunk to the league rate (prior 500 possessions).
On-ball expectation is then scaled by a size+role Poisson term on defender features (pair level).
Help: shooting fouls in the play-by-play beyond those on his matchups (fouling someone else's man), vs expected from a
  size+role regression across player-seasons (height, weight, own 3PA and rim share, usage, assists per possession).
Points: fouls x the season's league points given up per shooting foul (def_reaves.py value)."""
import pandas as pd, numpy as np, glob
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'; PRIOR=500
S=pd.read_csv(f'{OUT}/def_reaves_seasons.csv')
val=(S.groupby('season').fv.sum()/S.groupby('season').fouls.sum())          # points per shooting foul (above an unfouled attempt)
rows=[]; PAIRS=[]
for y in range(2017,2026):
    m=pd.read_csv(f'{D}/mu/matchups_{y}.csv',usecols=['person_id','matchups_person_id','partial_possessions','matchup_field_goals_attempted','shooting_fouls'])
    m.columns=['off','dfn','poss','fga','sf']; m=m[(m.off>0)&(m.dfn>0)]
    m=m.groupby(['off','dfn'],as_index=False)[['poss','fga','sf']].sum()
    lg=m.sf.sum()/m.poss.sum()
    o=m.groupby('off')[['poss','sf']].sum().rename(columns={'poss':'op','sf':'os'})
    m=m.join(o,on='off')
    m['rate']=(m.os-m.sf+lg*PRIOR)/(m.op-m.poss+PRIOR)                          # leave this defender out
    m['x']=m.rate*m.poss
    m['season']=y; PAIRS.append(m[['off','dfn','season','poss','sf','x']])
    d=m.groupby('dfn').agg(mu_poss=('poss','sum'),mu_fga=('fga','sum'),mu_sf=('sf','sum'),mu_x=('x','sum')).reset_index().rename(columns={'dfn':'pid'})
    d['season']=y; rows.append(d); print(y,'pairs',len(m),'league sf/poss',round(lg,4),flush=True)
MU=pd.concat(rows)
T=S[S.season>=2017][['pid','season','name','def_poss','fouls','xf_r','dreaves_r']].merge(MU,on=['pid','season'],how='inner')
T['help']=(T.fouls-T.mu_sf).clip(lower=0)
# help-foul expectation: WLS of help fouls per def poss on size + role, weighted by def poss
H=pd.read_csv(f'{D}/ext/heights.csv')[['shooter','height_in','weight']].rename(columns={'shooter':'pid'})
R0=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','season','off_poss']].rename(columns={'shooter':'pid'})
XA=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['season','shooter','z','kind']); XA=XA[(XA.kind!='fouled_miss')&(XA.season>=2017)]
zs=XA.assign(t3=XA.z.isin(['C3','AB3']),rim=XA.z.isin(['RA','PAINT'])).groupby(['shooter','season']).agg(t3=('t3','mean'),rimsh=('rim','mean'),fga=('z','size')).reset_index().rename(columns={'shooter':'pid'})
HD=pd.concat([pd.read_parquet(f) for f in glob.glob(f'{D}/handling/handling_*.parquet')])
T=T.merge(H,on='pid',how='left').merge(R0,on=['pid','season'],how='left').merge(zs,on=['pid','season'],how='left').merge(HD,on=['pid','season'],how='left')
T=T[T.def_poss>0].copy()
T['usage']=T.fga/T.off_poss; T['ast_rate']=T.ast/T.off_poss
T.loc[T.off_poss.fillna(0)<200,['usage','ast_rate','t3','rimsh']]=np.nan
f=['height_in','weight','t3','rimsh','usage','ast_rate']
Z=T[f].copy()
for c in f: Z[c]=Z[c].fillna(Z[c].median()); Z[c]=(Z[c]-Z[c].mean())/Z[c].std()
Xm=np.column_stack([np.ones(len(T)),Z.to_numpy(),Z.to_numpy()**2])
w=T.def_poss.to_numpy(); yv=(T.help/T.def_poss).to_numpy()
b=np.linalg.lstsq(Xm*np.sqrt(w)[:,None],yv*np.sqrt(w),rcond=None)[0]
T['help_x']=np.clip(Xm@b,0,None)*T.def_poss
r2=1-np.average((yv-Xm@b)**2,weights=w)/np.average((yv-np.average(yv,weights=w))**2,weights=w); print('help model weighted R2',round(r2,3))
# ---- size+role adjustment of the on-ball expectation: Poisson on defender x shooter pairs,
#  sf ~ Poisson(x * exp(b . defender features)), x = shooter's leave-out rate x possessions (the offset)
P=pd.concat(PAIRS,ignore_index=True)
FZ=T[['pid','season']].copy(); FZ[[c+'_z' for c in f]]=Z.to_numpy()
for c in f: FZ[c+'_q']=FZ[c+'_z']**2
fc=[c+'_z' for c in f]+[c+'_q' for c in f]
P=P.merge(FZ,left_on=['dfn','season'],right_on=['pid','season'],how='left'); P[fc]=P[fc].fillna(0)
Xp=P[fc].to_numpy(); xo=P.x.to_numpy(); yo=P.sf.to_numpy()
from scipy.optimize import minimize
def nll(bb):
    a,bb_=bb[0],bb[1:]; mu=xo*np.exp(a+Xp@bb_); return (mu-yo*np.log(mu+1e-300)).sum()
def gr(bb):
    a,bb_=bb[0],bb[1:]; mu=xo*np.exp(a+Xp@bb_); r=mu-yo; return np.concatenate([[r.sum()],Xp.T@r])
bp=minimize(nll,np.zeros(1+len(fc)),jac=gr,method='L-BFGS-B').x
print('on-ball role model coefs',dict(zip(['const']+fc,bp.round(3))))
P['x_adj']=xo*np.exp(bp[0]+Xp@bp[1:])
T=T.merge(P.groupby(['dfn','season']).x_adj.sum().rename('mu_x_adj'),left_on=['pid','season'],right_index=True,how='left')
T['mu_x_raw']=T.mu_x; T['mu_x']=T.mu_x_adj
v=T.season.map(val)
T['onball_ex']=T.mu_sf-T.mu_x; T['help_ex']=T.help-T.help_x
T['mu_x_total']=T.mu_x+T.help_x; T['mu_ratio']=T.fouls/T.mu_x_total
T['mu_dreaves']=(T.onball_ex+T.help_ex)*v; T['onball_pts']=T.onball_ex*v; T['help_pts']=T.help_ex*v
T['mu_dreaves_per100poss']=100*T.mu_dreaves/T.def_poss
T.to_csv(f'{OUT}/def_reaves_matchup_seasons.csv',index=False)
# agreement with the lineup-based size+role version
k=T[T.def_poss>=2000]
print('season-level r, matchup vs lineup size+role (per 100 def poss):',round(k.mu_dreaves_per100poss.corr(100*k.dreaves_r/k.def_poss),3),len(k))
kk=k.merge(k.assign(season=k.season-1),on=['pid','season'],suffixes=('','_n'))
print('year-to-year r: matchup',round(kk.mu_dreaves_per100poss.corr(kk.mu_dreaves_per100poss_n),3),' lineup',round((kk.dreaves_r/kk.def_poss).corr(kk.dreaves_r_n/kk.def_poss_n),3))
C=T.groupby('pid').agg(name=('name','first'),first=('season','min'),last=('season','max'),def_poss=('def_poss','sum'),fouls=('fouls','sum'),onball=('mu_sf','sum'),
   onball_x=('mu_x','sum'),onball_x_raw=('mu_x_raw','sum'),help=('help','sum'),help_x=('help_x','sum'),mu_dreaves=('mu_dreaves','sum'),onball_pts=('onball_pts','sum'),help_pts=('help_pts','sum'),dreaves_r=('dreaves_r','sum')).reset_index()
C['ratio']=C.fouls/(C.onball_x+C.help_x); C['onball_ratio_raw']=C.onball/C.onball_x_raw; C['raw_per100poss']=100*((C.onball-C.onball_x_raw)+(C.help-C.help_x))*0.6/C.def_poss; C['onball_ratio']=C.onball/C.onball_x; C['help_ratio']=C.help/C.help_x
C['mu_dreaves_per100poss']=100*C.mu_dreaves/C.def_poss; C['lineup_per100poss']=100*C.dreaves_r/C.def_poss
C['se_per100']=100*np.sqrt(C.onball_x+C.help_x)*0.6/C.def_poss
C=C[C.def_poss>=10000]; C.to_csv(f'{OUT}/def_reaves_matchup_career.csv',index=False)
c=['name','first','last','def_poss','fouls','ratio','onball_ratio','onball_ratio_raw','help_ratio','mu_dreaves','mu_dreaves_per100poss','lineup_per100poss']
print('players',len(C),' career r matchup vs lineup',round(C.mu_dreaves_per100poss.corr(C.lineup_per100poss),3))
print('MOST (per 100 def poss)'); print(C.sort_values('mu_dreaves_per100poss',ascending=False)[c].head(20).round(2).to_string(index=False))
print('FEWEST'); print(C.sort_values('mu_dreaves_per100poss')[c].head(20).round(2).to_string(index=False))
print('MOST total pts'); print(C.sort_values('mu_dreaves',ascending=False)[c].head(10).round(2).to_string(index=False))
