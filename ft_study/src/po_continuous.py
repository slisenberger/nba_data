"""Continuous usage model for playoff riser/dropper effects (total scoring value per game, po_pergame_seasons.csv).
Player mean residual gap m_i ~ N(mu + g*z_i, v_i + tau2(z_i)), tau2(z) = exp(a + b*z), z = standardized career RS usage.
Fit by maximum likelihood; likelihood-ratio tests for b = 0 and for tau = 0; same model on the RS-halves placebo.
Each player shrunk with his own tau2(z_i). Output results/po_continuous_players.csv"""
import pandas as pd, numpy as np
from scipy.optimize import minimize
from scipy.stats import chi2
OUT='/home/user/nba_data/ft_study/results'
W=pd.read_csv(f'{OUT}/po_pergame_seasons.csv')
import os, glob
if os.environ.get('LOAD')=='1':
    # offensive load: (TS attempts + turnovers + assists) per 100 on-court possessions, regular season
    D='/home/claude/data'; R=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','season','att','off_poss']]
    HD=pd.concat([pd.read_parquet(f) for f in glob.glob(f'{D}/handling/handling_*.parquet')]).rename(columns={'pid':'shooter'})
    TS=[]
    for y in range(1996,2025):
        a_=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['shooter','kind','fta']); a_=a_[a_.shooter>0]
        TS.append(a_.assign(tsa=a_.kind.isin(['fga','and1']).astype(float)+0.44*a_.fta).groupby('shooter').tsa.sum().reset_index().assign(season=y))
    TS=pd.concat(TS)
    L=R[['shooter','season','off_poss']].merge(TS,on=['shooter','season']).merge(HD,on=['shooter','season'],how='left').fillna({'ast':0,'tov':0})
    L['load']=100*(L.tsa+L.tov+L.ast)/L.off_poss
    W=W.drop(columns=['usage_rs']).merge(L[['shooter','season','load']].rename(columns={'load':'usage_rs'}),on=['shooter','season'],how='left')
    OUT_SUF='_load'
else: OUT_SUF=''
def summ(col,v,k=4):
    p=W.groupby('shooter').apply(lambda g: pd.Series({'m':np.average(g[col],weights=1/g[v]),'v':1/np.sum(1/g[v]),'k':len(g),'name':g['name'].iloc[0],'usage':g.usage_rs.mean()})); p=p[p.k>=k].dropna(subset=['usage'])
    for c in ['m','v','usage']: p[c]=p[c].astype(float)
    return p
def nll(th,m,v,z,free_b=True,free_tau=True,free_g=True):
    mu,g,a,b=th; g=g if free_g else 0.0; b=b if free_b else 0.0
    t2=np.exp(a+b*z) if free_tau else 0.0
    s=v+t2; return 0.5*np.sum(np.log(s)+(m-mu-g*z)**2/s)
def fit(p,**kw):
    z=((p.usage-p.usage.mean())/p.usage.std()).to_numpy(); m=p.m.to_numpy(); v=p.v.to_numpy()
    best=None
    for a0 in [-6,-3,-1]:
        r=minimize(lambda th: nll(th,m,v,z,**kw),[0,0,a0,0],method='Nelder-Mead',options={'maxiter':20000,'xatol':1e-7,'fatol':1e-9})
        if best is None or r.fun<best.fun: best=r
    return best,z
for lab,col,v in [('PLAYOFFS','r','vg'),('PLACEBO','rp','vgp')]:
    p=summ(col,v); full,z=fit(p); nob,_=fit(p,free_b=False); notau,_=fit(p,free_tau=False,free_b=False)
    mu,g,a,b=full.x; lr_b=2*(nob.fun-full.fun); lr_tau=2*(notau.fun-full.fun)
    sd=lambda zz: np.sqrt(np.exp(a+b*zz))
    print(f'{lab}: players {len(p)}; mean gap {mu:+.3f} + {g:+.3f} per SD usage; persistent SD at usage -1 SD {sd(-1):.2f}, mean {sd(0):.2f}, +1 SD {sd(1):.2f}, +2 SD {sd(2):.2f} pts/game')
    print(f'   LR test usage-dependence (b=0): {lr_b:.2f}, p={1-chi2.cdf(lr_b,1):.3f};  any persistent effect (tau=0, df 2, boundary): {lr_tau:.2f}, p~{0.5*(1-chi2.cdf(lr_tau,1))+0.5*(1-chi2.cdf(lr_tau,2)):.3f}')
    if lab=='PLAYOFFS':
        t2=np.exp(a+b*z); prior=mu+g*z; P=p.copy(); P['tau']=np.sqrt(t2); P['se']=np.sqrt(P.v); P['prior']=prior; P['indiv']=t2/(t2+P.v)*(P.m-prior); P['shrunk']=prior+P.indiv; P['z_usage']=z
P.sort_values('shrunk').round(3).to_csv(f'{OUT}/po_continuous_players{OUT_SUF}.csv')
c=['name','k','usage','m','se','prior','indiv','shrunk']
print('\nCLIMBERS'); print(P.sort_values('shrunk',ascending=False)[c].head(12).round(2).to_string(index=False))
print('\nFALLERS'); print(P.sort_values('shrunk')[c].head(10).round(2).to_string(index=False))
print('\nnamed:'); print(P[P.name.isin(['Kawhi Leonard','James Harden','LeBron James','Kobe Bryant','Tim Duncan','Karl Malone','John Stockton','Dirk Nowitzki','Nikola Jokic','Reggie Miller','Chris Paul','Kevin Durant','Stephen Curry','Giannis Antetokounmpo'])][c].sort_values('shrunk').round(2).to_string(index=False))

print('\nINDIVIDUAL effect beyond what usage predicts (shrunk): top and bottom')
print(P.sort_values('indiv',ascending=False)[c].head(10).round(2).to_string(index=False)); print(P.sort_values('indiv')[c].head(8).round(2).to_string(index=False))
