"""Playoff value persistence by usage tier (career RS usage of playoff seasons, tertiles), each tier with its own placebo;
star rankings shrunk with the high-usage tier's own spread. Input po_robust_seasons.csv + po_droppers2_seasons.csv."""
import pandas as pd, numpy as np
from scipy.stats import chi2
OUT='/home/user/nba_data/ft_study/results'
W=pd.read_csv(f'{OUT}/po_robust_seasons.csv').merge(pd.read_csv(f'{OUT}/po_droppers2_seasons.csv')[['shooter','season','usage_rs']],on=['shooter','season'],how='left')
pu=W.groupby('shooter').usage_rs.mean()
def summ(d,col,v,k=4):
    p=d.groupby('shooter').apply(lambda g: pd.Series({'m':np.average(g[col],weights=1/g[v]),'v':1/np.sum(1/g[v]),'k':len(g),'name':g['name'].iloc[0]})); p=p[p.k>=k]; p['m']=p.m.astype(float); p['v']=p.v.astype(float); return p
def persist(p):
    def Qt(t2): w=1/(p.v+t2); mb=np.sum(w*p.m)/w.sum(); return np.sum(w*(p.m-mb)**2)
    Q=Qt(0); df=len(p)-1; w=1/p.v; tau2=max(0,(Q-df)/(w.sum()-np.sum(w**2)/w.sum())); return Q,df,1-chi2.cdf(Q,df),np.sqrt(tau2)
pa=summ(W,'r','vg'); pp=summ(W,'rp','vgp'); q=pu.reindex(pa.index); cut=q.quantile([1/3,2/3]).to_numpy(); out=[]
for lab,m in [('low',q<cut[0]),('mid',(q>=cut[0])&(q<cut[1])),('high',q>=cut[1])]:
    ids=q[m].index; a=persist(pa.loc[ids]); b=persist(pp.loc[pp.index.intersection(ids)]); print(lab,len(ids),[round(x,3) for x in a],'placebo',[round(x,3) for x in b])
    P=pa.loc[ids].copy(); P['tier']=lab; P['shrunk']=P.m*a[3]**2/(a[3]**2+P.v) if a[3]>0 else 0.0; P['se']=np.sqrt(P.v); out.append(P)
pd.concat(out).sort_values('shrunk').round(3).to_csv(f'{OUT}/po_usage_tiers_players.csv')
