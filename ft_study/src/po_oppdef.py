"""Opponent-defense-adjusted playoff TS gap. Each defense's RS TS% allowed vs league (dTS). For each player-season-segment
(np = RS vs non-playoff teams, pl = RS vs playoff teams, po = playoffs; npA/npB odd/even halves of np) the TSA-weighted mean dTS
of the defenses faced. kappa = how much a player's relative TS moves per point of opponent dTS, estimated from RS segments
(pl vs np, within player-season). Adjusted rTS = rTS - kappa x opp dTS. Gap = adj rTS_po - adj rTS_pl; minimal model: league drop
+ talent (adj rTS_np). Persistence vs placebo; named players before/after."""
import pandas as pd, numpy as np
from scipy.stats import chi2
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'; SD1=0.575
rows=[]
for y in range(1996,2025):
    po=pd.read_parquet(f'{D}/out_po/att_{y}.parquet',columns=['off','deff','shooter','kind','pts','fta']); pot=set(po.off.unique())
    rs=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['deff','shooter','kind','pts','fta','game_id'])
    for a in (rs,po): a['tsa']=a.kind.isin(['fga','and1']).astype(float)+0.44*a.fta
    lg=rs.pts.sum()/(2*rs.tsa.sum()); dts=(rs.groupby('deff').pts.sum()/(2*rs.groupby('deff').tsa.sum())-lg)*100
    rs['seg']=np.where(rs.deff.isin(pot),'pl','np'); r2=rs[rs.seg=='np'].copy(); r2['seg']=np.where(r2.game_id%2==1,'npA','npB'); po['seg']='po'
    a=pd.concat([rs.drop(columns='game_id'),r2.drop(columns='game_id'),po.drop(columns='off')]); a=a[a.shooter>0]; a['od']=a.deff.map(dts)*a.tsa
    lgs=a.groupby('seg').apply(lambda g: g.pts.sum()/(2*g.tsa.sum()))
    g=a.groupby(['shooter','seg']).agg(pts=('pts','sum'),tsa=('tsa','sum'),od=('od','sum')).reset_index(); g['lg']=g.seg.map(lgs); g['season']=y; rows.append(g)
G=pd.concat(rows); G['rts']=100*(G.pts/(2*G.tsa)-G.lg); G['opp']=G.od/G.tsa
W=G.pivot_table(index=['shooter','season'],columns='seg',values=['rts','opp','tsa']); W.columns=[a+'_'+b for a,b in W.columns]; W=W.reset_index()
W=W[(W.tsa_po>=50)&(W.tsa_pl>=100)&(W.tsa_npA>=50)&(W.tsa_npB>=50)].copy()
nm=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','name']].drop_duplicates('shooter').set_index('shooter').name; W['name']=W.shooter.map(nm)
# kappa from RS: (rts_pl - rts_np) on (opp_pl - opp_np), weights by precision
dy=W.rts_pl-W.rts_np; dx=W.opp_pl-W.opp_np; w=1/((100*SD1)**2*(1/W.tsa_pl+1/W.tsa_np))
kappa=np.sum(w*(dx-np.average(dx,weights=w))*(dy-np.average(dy,weights=w)))/np.sum(w*(dx-np.average(dx,weights=w))**2)
kappa_seg=kappa; kappa=0.86   # from player x opponent pairs (leave-one-out, RS 2005/10/15/20/23); the segment estimate is attenuated
# opponent strength relative to the average opponent of that season-segment (segment league averages already absorb the mean)
for sname in ['po','pl','np','npA','npB']:
    mo=W.groupby('season').apply(lambda g: np.average(g['opp_'+sname],weights=g['tsa_'+sname])); W['opp_'+sname]=W['opp_'+sname]-W.season.map(mo)
print(f'segment-based kappa {kappa_seg:.2f} (attenuated); using pair-based 0.86')
print(f'kappa (TS points per point of opponent TS allowed) = {kappa:.2f};  opponent dTS: RS vs playoff teams {np.average(W.opp_pl,weights=W.tsa_po):+.2f}, playoffs {np.average(W.opp_po,weights=W.tsa_po):+.2f}; spread of playoff-minus-control opponent strength (sd) {np.std(W.opp_po-W.opp_pl):.2f}')
for s in ['po','pl','np','npA','npB']: W['adj_'+s]=W['rts_'+s]-kappa*W['opp_'+s]
W['v']=(100*SD1)**2*(1/W.tsa_po+1/W.tsa_pl); W['vp']=(100*SD1)**2*(1/W.tsa_pl+1/W.tsa_npA)
def resid(y,x,v):
    X=np.column_stack([np.ones(len(W)),W[x]]); ww=1/W[v].to_numpy(); sw=np.sqrt(ww); b=np.linalg.lstsq(X*sw[:,None],W[y].to_numpy()*sw,rcond=None)[0]; return b,W[y].to_numpy()-X@b
W['g_raw']=W.rts_po-W.rts_pl; W['g_adj']=W.adj_po-W.adj_pl; W['gp_adj']=W.adj_pl-W.adj_npA
b0,W['r_raw']=resid('g_raw','rts_np','v'); b1,W['r_adj']=resid('g_adj','adj_np','v'); bp,W['rp_adj']=resid('gp_adj','adj_npB','vp')
print(f'league-wide playoff TS gap: unadjusted {b0[0]:+.2f}, opponent-adjusted {b1[0]:+.2f} (placebo {bp[0]:+.2f})')
def summ(col,v,k=4):
    p=W.groupby('shooter').apply(lambda g: pd.Series({'m':np.average(g[col],weights=1/g[v]),'v':1/np.sum(1/g[v]),'k':len(g),'name':g['name'].iloc[0]})); p=p[p.k>=k]; p['m']=p.m.astype(float); p['v']=p.v.astype(float); return p
def persist(p):
    def Qt(t2): w=1/(p.v+t2); mb=np.sum(w*p.m)/w.sum(); return np.sum(w*(p.m-mb)**2)
    Q=Qt(0); df=len(p)-1; w=1/p.v; tau2=max(0,(Q-df)/(w.sum()-np.sum(w**2)/w.sum())); return Q,df,1-chi2.cdf(Q,df),np.sqrt(tau2)
for lab,c,v in [('unadjusted','r_raw','v'),('opponent-adjusted','r_adj','v'),('placebo (adjusted)','rp_adj','vp')]:
    p=summ(c,v); q=persist(p); print(f'{lab:20s}: players {len(p)}, Q {q[0]:.0f}/{q[1]}, p {q[2]:.3f}, persistent SD {q[3]:.2f} TS pts')
pu=summ('r_raw','v'); pa=summ('r_adj','v'); J=pu[['name','k','m']].rename(columns={'m':'unadj'}).join(pa[['m','v']].rename(columns={'m':'adj'})); J['se']=np.sqrt(J.v); J['change']=J.adj-J.unadj
J.drop(columns='v').round(2).to_csv(f'{OUT}/po_oppdef_players.csv')
print('\nbiggest changes from the adjustment'); print(J.reindex(J.change.abs().sort_values(ascending=False).index)[['name','k','unadj','adj','se']].head(8).round(2).to_string(index=False))
ch=['James Harden','John Stockton','Karl Malone','Reggie Miller','LeBron James','Kawhi Leonard','Dirk Nowitzki','Nikola Jokic','Kobe Bryant','Tim Duncan','Chris Paul','Kevin Durant','Stephen Curry','Giannis Antetokounmpo','Joel Embiid']
print('\nnamed (TS points, playoffs vs own RS, after league drop)'); print(J[J.name.isin(ch)].sort_values('adj')[['name','k','unadj','adj','se']].round(2).to_string(index=False))
print('\nopponent strength faced, playoffs minus control (TS allowed, + = weaker defenses):')
o=W.groupby('shooter').apply(lambda g: pd.Series({'name':g['name'].iloc[0],'d':np.average(g.opp_po-g.opp_pl,weights=g.tsa_po)})); print(o[o.name.isin(ch)].sort_values('d').round(2).to_string(index=False))
W.round(3).to_csv(f'{OUT}/po_oppdef_seasons.csv',index=False)
