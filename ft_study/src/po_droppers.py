"""Do some players reliably over- or under-perform in the playoffs? (scoring efficiency)
Per player-season: TS% = points / (2 x (FGA + 0.44 FTA)) in three segments: regular-season games vs non-playoff teams (base),
regular-season games vs that season's playoff teams (control = same opponents' calibre), playoffs.
Gap = playoff TS - control TS (both relative to the segment's league TS). Persistence test: is a player's gap in one playoff
run correlated with his gap in other runs? Variance of the persistent player component estimated two ways:
  1) random-effects (DerSimonian-Laird) on players' mean gaps with sampling SEs,
  2) split-half: odd vs even playoff seasons per player, Spearman-Brown corrected.
Placebo: same machinery on RS-vs-playoff-teams minus base (no playoffs involved)."""
import pandas as pd, numpy as np
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
rows=[]
for y in range(1996,2025):
    po=pd.read_parquet(f'{D}/out_po/att_{y}.parquet',columns=['off','shooter','kind','pts','fta']); pot=set(po.off.unique())
    rs=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['deff','shooter','kind','pts','fta','game_id'])
    rs['seg']=np.where(rs.deff.isin(pot),'pl','np'); po['seg']='po'
    a=pd.concat([rs[['shooter','kind','pts','fta','seg']],po[['shooter','kind','pts','fta','seg']]]); a=a[a.shooter>0]
    a['tsa']=a.kind.isin(['fga','and1']).astype(float)+0.44*a.fta
    lg=a.groupby('seg').apply(lambda g: g.pts.sum()/(2*g.tsa.sum()))
    g=a.groupby(['shooter','seg']).agg(pts=('pts','sum'),tsa=('tsa','sum')).reset_index(); g['lg']=g.seg.map(lg); g['season']=y; rows.append(g)
G=pd.concat(rows)
W=G.pivot_table(index=['shooter','season'],columns='seg',values=['pts','tsa','lg']); W.columns=[a+'_'+b for a,b in W.columns]; W=W.reset_index()
for s in ['np','pl','po']: W['rts_'+s]=100*(W['pts_'+s]/(2*W['tsa_'+s])-W['lg_'+s])
# sampling variance of TS per TSA (points per TSA has sd ~ 1.3 -> TS sd ~ 0.65 per attempt) estimated from data
sd1=0.575   # empirical per-TSA sd of TS (2017-18 attempts)
W=W[(W.tsa_po>=50)&(W.tsa_pl>=100)&(W.tsa_np>=150)].copy()
W['gap']=W.rts_po-W.rts_pl; W['v']=(100*sd1)**2*(1/W.tsa_po+1/W.tsa_pl)
W['gap_pl']=W.rts_pl-W.rts_np; W['v_pl']=(100*sd1)**2*(1/W.tsa_pl+1/W.tsa_np)
nm=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','name']].drop_duplicates('shooter').set_index('shooter').name; W['name']=W.shooter.map(nm)
print('player-seasons',len(W),' league: mean playoff gap (TS pts, attempt-weighted)',round(np.average(W.gap,weights=W.tsa_po),2))
# check the sampling sd assumption: variance of gaps for tiny-signal case ~ mean v (placebo should be close)
def dl(d,gc,vc):
    p=d.groupby('shooter').apply(lambda g: pd.Series({'m':np.average(g[gc],weights=1/g[vc]),'v':1/np.sum(1/g[vc]),'k':len(g)}))
    p=p[p.k>=4]; w=1/p.v; mbar=np.sum(w*p.m)/w.sum(); Qs=np.sum(w*(p.m-mbar)**2); dfree=len(p)-1
    tau2=max(0,(Qs-dfree)/(w.sum()-np.sum(w**2)/w.sum())); return p,tau2,Qs,dfree
for lab,gc,vc in [('PLAYOFFS (playoff - control)','gap','v'),('PLACEBO (control - base, all regular season)','gap_pl','v_pl')]:
    p,tau2,Qs,dfree=dl(W,gc,vc)
    d=W.copy(); d['r']=d.groupby('shooter').season.rank(); d=d[d.shooter.isin(p.index)]
    od=d[d.r%2==1].groupby('shooter').apply(lambda g: np.average(g[gc],weights=1/g[vc])); ev=d[d.r%2==0].groupby('shooter').apply(lambda g: np.average(g[gc],weights=1/g[vc]))
    j=od.index.intersection(ev.index); r=np.corrcoef(od[j],ev[j])[0,1]; sb=2*r/(1+r)
    print(f'{lab}: players with >=4 runs {len(p)}, Q={Qs:.0f} on {dfree} df, persistent player SD = {np.sqrt(tau2):.2f} TS pts; split-half r {r:.3f} (Spearman-Brown {sb:.3f})')
    if gc=='gap': P=p.join(W.groupby('shooter').name.first())
# empirical-Bayes player estimates
_,tau2,_,_=dl(W,'gap','v')
P['shrunk']=P.m*tau2/(tau2+P.v); P['se']=np.sqrt(P.v)
P=P.sort_values('shrunk'); P.round(3).to_csv(f'{OUT}/po_droppers_players.csv')
c=['name','k','m','se','shrunk']
print('largest shrunk under-performers'); print(P[c].head(10).round(2).to_string())
print('largest shrunk over-performers'); print(P[c].tail(10).round(2).to_string())
W.round(3).to_csv(f'{OUT}/po_droppers_seasons.csv',index=False)
