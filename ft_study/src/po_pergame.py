"""Playoff over/under-performance in TOTAL scoring value per game (points above league-average efficiency per game, turnovers
included): rewards both efficiency and taking on more possessions. From po_robust_seasons.csv (value per 100 possessions and
possessions) plus games per segment from the possession tables.
Gap = per-game value playoffs - RS vs playoff teams; minus a + b x per-game value vs other teams. Placebo: RS vs playoff
teams - odd games, talent from even games. Persistence overall and by usage tier, rankings within tier."""
import pandas as pd, numpy as np
from scipy.stats import chi2
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
W=pd.read_csv(f'{OUT}/po_robust_seasons.csv')
G=[]
for y in range(1996,2025):
    pot=set(pd.read_parquet(f'{D}/out_po/att_{y}.parquet',columns=['off']).off.unique())
    for src,ispo in (('out',False),('out_po',True)):
        p=pd.read_parquet(f'{D}/{src}/poss_{y}.parquet',columns=['game_id','deff','off_lu','lq']); p=p[p.lq==1]
        p['seg']='po' if ispo else np.where(p.deff.isin(pot),'pl',np.where(p.game_id%2==1,'npA','npB'))
        n=p.off_lu.map(len).to_numpy(); v=pd.DataFrame({'shooter':np.concatenate(p.off_lu.to_numpy()),'seg':np.repeat(np.asarray(p.seg),n),'g':np.repeat(p.game_id.to_numpy(),n)})
        g=v.groupby(['shooter','seg']).g.nunique().rename('games').reset_index()
        if not ispo:
            gn=g[g.seg.isin(['npA','npB'])].groupby('shooter').games.sum().reset_index().assign(seg='np'); g=pd.concat([g,gn])
        g['season']=y; G.append(g)
G=pd.concat(G).pivot_table(index=['shooter','season'],columns='seg',values='games'); G.columns=['games_'+c for c in G.columns]
W=W.merge(G.reset_index(),on=['shooter','season'],how='left')
for s in ['po','pl','np','npA','npB']:
    W['pg_'+s]=W['V_'+s]*W['poss_'+s]/100/W['games_'+s]; W['vpg_'+s]=W['v_'+s]*(W['poss_'+s]/100/W['games_'+s])**2
W=W.dropna(subset=[c for c in W.columns if c.startswith('pg_')])
def resid(y,x,v):
    X=np.column_stack([np.ones(len(W)),W[x]]); w=1/W[v].to_numpy(); sw=np.sqrt(w); b=np.linalg.lstsq(X*sw[:,None],W[y].to_numpy()*sw,rcond=None)[0]; return b,W[y].to_numpy()-X@b
W['gap']=W.pg_po-W.pg_pl; W['vg']=W.vpg_po+W.vpg_pl; b,W['r']=resid('gap','pg_np','vg')
W['gpl']=W.pg_pl-W.pg_npA; W['vgp']=W.vpg_pl+W.vpg_npA; bp,W['rp']=resid('gpl','pg_npB','vgp')
print(f'league-wide playoff gap {b[0]:+.2f} points per game (slope {b[1]:+.2f}); placebo {bp[0]:+.2f}; player-seasons {len(W)}')
print('minutes load: on-court possessions per game, playoffs vs RS vs playoff teams (median ratio)',round((W.poss_po/W.games_po/(W.poss_pl/W.games_pl)).median(),3))
U=pd.read_csv(f'{OUT}/po_droppers2_seasons.csv')[['shooter','season','usage_rs']]; W=W.merge(U,on=['shooter','season'],how='left')
def summ(d,col,v,k=4):
    p=d.groupby('shooter').apply(lambda g: pd.Series({'m':np.average(g[col],weights=1/g[v]),'v':1/np.sum(1/g[v]),'k':len(g),'name':g['name'].iloc[0],'usage':g.usage_rs.mean()})); p=p[p.k>=k]
    for c in ['m','v','usage']: p[c]=p[c].astype(float)
    return p
def persist(p):
    def Qt(t2): w=1/(p.v+t2); mb=np.sum(w*p.m)/w.sum(); return np.sum(w*(p.m-mb)**2)
    Q=Qt(0); df=len(p)-1; w=1/p.v; tau2=max(0,(Q-df)/(w.sum()-np.sum(w**2)/w.sum())); return Q,df,1-chi2.cdf(Q,df),np.sqrt(tau2)
pa=summ(W,'r','vg'); pp=summ(W,'rp','vgp')
a=persist(pa); bq=persist(pp); print(f'ALL: players {len(pa)} Q {a[0]:.0f}/{a[1]} p {a[2]:.3f} SD {a[3]:.2f} pts/game | placebo p {bq[2]:.3f} SD {bq[3]:.2f}')
cut=pa.usage.quantile([1/3,2/3]).to_numpy(); out=[]
for lab,m in [('low',pa.usage<cut[0]),('mid',(pa.usage>=cut[0])&(pa.usage<cut[1])),('high',pa.usage>=cut[1])]:
    ids=pa[m].index; a=persist(pa.loc[ids]); bq=persist(pp.loc[pp.index.intersection(ids)])
    print(f'{lab:5s} usage: players {len(ids)} Q {a[0]:.0f}/{a[1]} p {a[2]:.3f} SD {a[3]:.2f} | placebo p {bq[2]:.3f} SD {bq[3]:.2f}')
    P=pa.loc[ids].copy(); P['tier']=lab; P['se']=np.sqrt(P.v); P['shrunk']=P.m*a[3]**2/(a[3]**2+P.v) if a[3]>0 else 0.0; out.append(P)
P=pd.concat(out); P.sort_values('shrunk').round(3).to_csv(f'{OUT}/po_pergame_players.csv')
S=P[P.tier=='high']; c=['name','k','m','se','shrunk']
print('\nSTARS (high usage) climbers'); print(S.sort_values('shrunk',ascending=False)[c].head(10).round(2).to_string(index=False))
print('STARS fallers'); print(S.sort_values('shrunk')[c].head(8).round(2).to_string(index=False))
print('named:',', '.join(f"{r.name} {r.m:+.2f}±{r.se:.2f} -> {r.shrunk:+.2f}" for r in P[P.name.isin(['Kawhi Leonard','James Harden','LeBron James','Kobe Bryant','Tim Duncan','Karl Malone','John Stockton','Dirk Nowitzki','Nikola Jokic','Reggie Miller','Chris Paul','Kevin Durant','Stephen Curry'])].sort_values('m').itertuples()))
W.round(3).to_csv(f'{OUT}/po_pergame_seasons.csv',index=False)
