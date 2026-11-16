"""Do playoff whistles shrink more for foul-drawing specialists ("swallow the whistle on grifts")?
Per player-season, shooting-foul rate = (and-ones + fouled misses) / shooting attempts, separately for 2PT and 3PT attempts.
Base = regular-season games vs non-playoff teams. Targets: (placebo) regular-season games vs that season's playoff teams,
and the playoffs. Each player's base rate is shrunk (empirical Bayes, binomial noise) so regression to the mean is built in;
predicted target rate = league target/base factor x shrunk base rate. Residual (actual - predicted) by quintile of base
rate: if refs swallow the whistle on specialists, the playoff residual falls with the base rate and the placebo's doesn't.
Also 3PT fouls (the classic jump-shot grift) vs 2PT fouls."""
import pandas as pd, numpy as np
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
K=['fga','and1','fouled_miss']
rows=[]
for y in range(1996,2025):
    try: p=pd.read_parquet(f'{D}/out_po/att_{y}.parquet',columns=['shooter','kind','is3','off'])
    except FileNotFoundError: continue
    r=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['shooter','kind','is3','deff'])
    po_teams=set(p.off.unique())
    r=r[r.kind.isin(K)&(r.shooter>0)]; p=p[p.kind.isin(K)&(p.shooter>0)]
    r['seg']=np.where(r.deff.isin(po_teams),'rs_po','rs_np'); p['seg']='po'
    a=pd.concat([r[['shooter','kind','is3','seg']],p[['shooter','kind','is3','seg']]])
    a['f']=a.kind.isin(['and1','fouled_miss']).astype(int); a['t']=np.where(a.is3==1,'3','2')
    g=a.groupby(['shooter','seg','t']).agg(n=('f','size'),f=('f','sum')).reset_index(); g['season']=y; rows.append(g)
G=pd.concat(rows)
W=G.pivot_table(index=['shooter','season','t'],columns='seg',values=['n','f'],fill_value=0); W.columns=[a+'_'+b for a,b in W.columns]; W=W.reset_index()
def shrink(f,n):
    m=f.sum()/n.sum(); r=f/n; noise=(m*(1-m)/n); tau2=max(np.average((r-m)**2,weights=n)-np.average(noise,weights=n),1e-6)
    return m+tau2/(tau2+noise)*(r-m), m, tau2
out=[]
names=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','season','name']]
for t in ['2','3','all']:
    if t=='all': X=W.groupby(['shooter','season'])[[c for c in W.columns if c.startswith(('n_','f_'))]].sum().reset_index()
    else: X=W[W.t==t].copy()
    X=X[(X.n_rs_np>=(100 if t!='3' else 50))].copy()
    X['base'],m,tau2=shrink(X.f_rs_np,X.n_rs_np)
    X['q']=pd.qcut(X.base.rank(method='first'),5,labels=[1,2,3,4,5])
    for tgt in ['rs_po','po']:
        Y=X[X['n_'+tgt]>0]
        fac=Y['f_'+tgt].sum()/(Y.base*Y['n_'+tgt]).sum()          # league target / base
        for q,h in Y.groupby('q',observed=True):
            pred=(fac*h.base*h['n_'+tgt]).sum(); act=h['f_'+tgt].sum()
            out.append(dict(type=t,target=tgt,q=int(q),base_rate=np.average(h.base,weights=h['n_'+tgt]),n=h['n_'+tgt].sum(),act=act,pred=pred,
                            ratio=act/pred,se=np.sqrt(pred)/pred,league_factor=fac))
O=pd.DataFrame(out); O.round(4).to_csv(f'{OUT}/po_whistle_quintiles.csv',index=False)
print(O[['type','target','q','base_rate','n','act','pred','ratio','se','league_factor']].round(3).to_string(index=False))
# difference-in-differences: top quintile vs bottom, playoffs minus placebo
for t in ['2','3','all']:
    o=O[O.type==t].set_index(['target','q'])
    d_po=o.loc[('po',5),'ratio']-o.loc[('po',1),'ratio']; d_pl=o.loc[('rs_po',5),'ratio']-o.loc[('rs_po',1),'ratio']
    se=np.sqrt(sum(o.loc[k,'se']**2 for k in [('po',5),('po',1),('rs_po',5),('rs_po',1)]))
    print(t,'top-minus-bottom quintile: playoffs',round(d_po,3),' placebo',round(d_pl,3),' DiD',round(d_po-d_pl,3),'+-',round(se,3))
