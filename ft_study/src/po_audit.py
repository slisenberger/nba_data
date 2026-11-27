"""Robustness audit of po_value.py (playoff scoring value persistence).
1) Clustered sampling variance: per player-season-segment, ratio estimator of PA with game-level and (playoffs) series-level
   clusters; pooled design effects (clustered / iid variance) per segment; persistence test re-run with scaled variances.
2) Team confound: persistence of residual gaps using only pairs of playoff runs with DIFFERENT teams vs same team.
3) Exclude role players (RS usage below median) and garbage time-heavy cases: persistence among high-usage players only.
4) Era split: 1996-2009 vs 2010-2024 halves agree?"""
import pandas as pd, numpy as np
from scipy.stats import chi2
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
W=pd.read_csv(f'{OUT}/po_value_seasons.csv')
rows=[]
for y in range(1996,2025):
    po=pd.read_parquet(f'{D}/out_po/att_{y}.parquet',columns=['game_id','off','deff','shooter','kind','pts','fta']); pot=set(po.off.unique())
    rs=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','off','deff','shooter','kind','pts','fta'])
    rs['seg']=np.where(rs.deff.isin(pot),'pl',np.where(rs.game_id%2==1,'npA','npB')); po['seg']='po'
    a=pd.concat([rs,po]); a=a[a.shooter>0]; a['tsa']=a.kind.isin(['fga','and1']).astype(float)+0.44*a.fta
    lg=a.groupby('seg').apply(lambda g: g.pts.sum()/(2*g.tsa.sum())); a['ex']=a.pts-2*a.seg.map(lg)*a.tsa
    # possessions per player-game
    P=[]
    for src,seg_of in (('out',None),('out_po','po')):
        p=pd.read_parquet(f'{D}/{src}/poss_{y}.parquet',columns=['game_id','off','deff','off_lu','lq']); p=p[p.lq==1]; n=p.off_lu.map(len).to_numpy()
        P.append(pd.DataFrame({'shooter':np.concatenate(p.off_lu.to_numpy()),'game_id':np.repeat(p.game_id.to_numpy(),n),'off':np.repeat(p.off.to_numpy(),n),'deff':np.repeat(p.deff.to_numpy(),n)}).groupby(['shooter','game_id','off','deff']).size().rename('poss').reset_index())
    P=pd.concat(P)
    ga=a.groupby(['shooter','game_id','seg','off','deff']).agg(ex=('ex','sum'),n=('ex','size'),s2=('ex',lambda v:(v**2).sum())).reset_index()
    ga=ga.merge(P,on=['shooter','game_id','off','deff'],how='left'); ga['season']=y; rows.append(ga)
GA=pd.concat(rows).dropna(subset=['poss'])
GA['clu']=np.where(GA.seg=='po',GA.deff.astype(str),GA.game_id.astype(str))   # playoffs: cluster by series (opponent); RS: by game
def cvar(g,cl):
    gg=g.groupby(cl).agg(ex=('ex','sum'),poss=('poss','sum')); T=gg.poss.sum(); r=gg.ex.sum()/T; k=len(gg)
    return 1e4*(k/(k-1))*((gg.ex-r*gg.poss)**2).sum()/T**2 if k>1 else np.nan
rec=[]
for (s,y,seg),g in GA.groupby(['shooter','season','seg']):
    if seg not in ('po','pl','npA'): continue
    T=g.poss.sum(); iid=1e4*g.s2.sum()/T**2
    rec.append(dict(shooter=s,season=y,seg=seg,iid=iid,game=cvar(g,'game_id'),clu=cvar(g,'clu')))
V=pd.DataFrame(rec)
for seg in ['po','pl','npA']:
    v=V[V.seg==seg].dropna(); print(f'{seg}: design effect vs iid  game-clustered {np.median(v.game/v.iid):.2f}', f' series-clustered {np.median(v.clu/v.iid):.2f}' if seg=='po' else '')
de={seg:float(np.median((V[V.seg==seg].dropna().game/V[V.seg==seg].dropna().iid))) for seg in ['po','pl','npA']}
de_series=float(np.median((V[V.seg=='po'].dropna().clu/V[V.seg=='po'].dropna().iid)))
def persist(d,col,v):
    p=d.groupby('shooter').apply(lambda g: pd.Series({'m':np.average(g[col],weights=1/g[v]),'v':1/np.sum(1/g[v]),'k':len(g)})); p=p[p.k>=4]
    def Qt(t2): w=1/(p.v+t2); mb=np.sum(w*p.m)/w.sum(); return np.sum(w*(p.m-mb)**2)
    Q=Qt(0); df=len(p)-1; w=1/p.v; tau2=max(0,(Q-df)/(w.sum()-np.sum(w**2)/w.sum())); return len(p),Q,df,1-chi2.cdf(Q,df),np.sqrt(tau2)
print('\n1) persistence under different noise models (players, Q/df, p, persistent SD pts/100)')
for lab,fpo,fpl in [('iid (as reported)',1,1),('game-clustered',de['po'],de['pl']),('series-clustered playoffs',de_series,de['pl'])]:
    d=W.copy(); d['vg2']=d.v_po*fpo+d.v_pl*de['pl']*(fpl/de['pl']) ; d['vgp2']=d.v_pl*de['pl']*(fpl/de['pl'])+d.v_npA*(de['npA'] if fpl!=1 else 1)
    a1=persist(d,'r','vg2'); a2=persist(d,'rp','vgp2')
    print(f'  {lab:28s} playoffs: n {a1[0]}, Q {a1[1]:.0f}/{a1[2]}, p {a1[3]:.3f}, SD {a1[4]:.2f} | placebo: Q {a2[1]:.0f}/{a2[2]}, p {a2[3]:.3f}, SD {a2[4]:.2f}')
# 2) team confound: correlation of residual gaps between runs with same vs different team
tm=GA[GA.seg=='po'].groupby(['shooter','season']).apply(lambda g: g.groupby('off').poss.sum().idxmax()).rename('team').reset_index()
d=W.merge(tm,on=['shooter','season'],how='left')
pairs=[]
for s,g in d.groupby('shooter'):
    g=g.sort_values('season').to_dict('records')
    for i in range(len(g)):
        for j in range(i+1,len(g)): pairs.append((g[i]['r'],g[j]['r'],g[i]['team']==g[j]['team'],1/np.sqrt(g[i]['vg']*g[j]['vg'])))
Pp=pd.DataFrame(pairs,columns=['a','b','same','w'])
for lab,m in [('same team',Pp.same),('different team',~Pp.same)]:
    q=Pp[m]; print(f'\n2) pairs of runs, {lab}: n {len(q)}, weighted corr {np.cov(q.a,q.b,aweights=q.w)[0,1]/np.sqrt(np.cov(q.a,q.a,aweights=q.w)[0,1]*np.cov(q.b,q.b,aweights=q.w)[0,1]):.3f}')
# 3) high-usage only; 4) era halves
d=W.copy(); d['vg2']=d.v_po*de_series+d.v_pl*de['pl']
us=pd.read_csv(f'{OUT}/po_droppers2_seasons.csv')[['shooter','season','usage_rs']]; d=d.merge(us,on=['shooter','season'],how='left')
hi=d[d.usage_rs>=d.usage_rs.median()]; a=persist(hi,'r','vg2'); print(f'\n3) high-usage players only (series-clustered): n {a[0]}, Q {a[1]:.0f}/{a[2]}, p {a[3]:.3f}, SD {a[4]:.2f}')
for lab,m in [('1996-2009',d.season<=2009),('2010-2024',d.season>=2010)]:
    a=persist(d[m],'r','vg2'); print(f'4) {lab} (series-clustered): n {a[0]}, Q {a[1]:.0f}/{a[2]}, p {a[3]:.3f}, SD {a[4]:.2f}')
# 5) player-specific per-attempt variance (3-point shooters have more variance than dunkers)
pv=GA.groupby(['shooter','season','seg']).agg(n=('n','sum'),s2=('s2','sum'),ex=('ex','sum'),poss=('poss','sum')).reset_index()
pv['var1']=pv.s2/pv.n-(pv.ex/pv.n)**2; pv['vps']=1e4*pv.n*pv.var1/pv.poss**2
pw=pv.pivot_table(index=['shooter','season'],columns='seg',values='vps'); pw.columns=['vps_'+c for c in pw.columns]
d=W.merge(pw.reset_index(),on=['shooter','season'],how='left'); d['vg3']=d.vps_po+d.vps_pl; d['vgp3']=d.vps_pl+d.vps_npA
d=d.dropna(subset=['vg3','vgp3'])
a1=persist(d,'r','vg3'); a2=persist(d,'rp','vgp3')
print(f'\n5) player-specific variance: playoffs Q {a1[1]:.0f}/{a1[2]}, p {a1[3]:.3f}, SD {a1[4]:.2f} | placebo Q {a2[1]:.0f}/{a2[2]}, p {a2[3]:.3f}, SD {a2[4]:.2f}')
