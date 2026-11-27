"""Playoff value persistence and rankings restricted to large playoff samples (po_robust_seasons.csv):
runs with >=700 / >=1000 on-court playoff possessions, and players with >=6,000 career playoff possessions."""
import pandas as pd, numpy as np
from scipy.stats import chi2
OUT='/home/user/nba_data/ft_study/results'; W=pd.read_csv(f'{OUT}/po_robust_seasons.csv')
def summ(d,col,v,k):
    p=d.groupby('shooter').apply(lambda g: pd.Series({'m':np.average(g[col],weights=1/g[v]),'v':1/np.sum(1/g[v]),'k':len(g),'poss':g.poss_po.sum(),'name':g['name'].iloc[0]})); p=p[p.k>=k]; p['m']=p.m.astype(float); p['v']=p.v.astype(float); return p
def persist(p):
    def Qt(t2): w=1/(p.v+t2); mb=np.sum(w*p.m)/w.sum(); return np.sum(w*(p.m-mb)**2)
    Q=Qt(0); df=len(p)-1; w=1/p.v; tau2=max(0,(Q-df)/(w.sum()-np.sum(w**2)/w.sum())); return Q,df,1-chi2.cdf(Q,df),np.sqrt(tau2)
out=[]
for lab,thr,k in [('all',0,4),('runs>=700',700,3),('runs>=1000',1000,2)]:
    d=W[W.poss_po>=thr]; p=summ(d,'r','vg',k); Q,df,pv,tau=persist(p); _,_,pvp,taup=persist(summ(d,'rp','vgp',k))
    print(lab,len(p),round(Q),df,round(pv,3),round(tau,2),'placebo',round(pvp,3),round(taup,2))
    p['shrunk']=p.m*tau**2/(tau**2+p.v); p['se']=np.sqrt(p.v); p['sample']=lab; out.append(p)
p=summ(W,'r','vg',4); big=p[p.poss>=6000].copy(); Q,df,pv,tau=persist(big); big['shrunk']=big.m*tau**2/(tau**2+big.v); big['se']=np.sqrt(big.v); big['sample']='career>=6000poss'; out.append(big)
print('career>=6000',len(big),round(Q),df,round(pv,3),round(tau,2))
pd.concat(out).round(3).to_csv(f'{OUT}/po_bigsample_players.csv')
