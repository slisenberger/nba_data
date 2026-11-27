"""Playoff over/under-performance in scoring VALUE (volume x efficiency): points added above league-average efficiency per
100 on-court possessions, PA = 100 x 2 x TSA x (TS - league TS) / possessions, per segment (league = that segment's).
Extra volume at above-average efficiency adds value; extra volume at below-average efficiency subtracts.
Gap = PA_playoffs - PA_RS-vs-playoff-teams; model: gap = a + b PA_RS-vs-other-teams (league-wide drop + regression only).
Sampling variance from the per-attempt variance of (points - 2 x league TS x TSA). Persistence: random effects, split-half,
consecutive runs; placebo = RS vs playoff teams minus RS odd games, talent from RS even games. 1996-97 to 2024-25."""
import pandas as pd, numpy as np
from scipy.stats import chi2
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
rows=[]; VAR=[]
for y in range(1996,2025):
    po=pd.read_parquet(f'{D}/out_po/att_{y}.parquet',columns=['off','shooter','kind','pts','fta']); pot=set(po.off.unique())
    rs=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['deff','shooter','kind','pts','fta','game_id'])
    rs['seg']=np.where(rs.deff.isin(pot),'pl','np'); po['seg']='po'
    r2=rs[rs.seg=='np'].copy(); r2['seg']=np.where(r2.game_id%2==1,'npA','npB')
    a=pd.concat([rs.drop(columns=['deff','game_id']),r2.drop(columns=['deff','game_id']),po.drop(columns='off')]); a=a[a.shooter>0]
    a['tsa']=a.kind.isin(['fga','and1']).astype(float)+0.44*a.fta
    lg=a.groupby('seg').apply(lambda g: g.pts.sum()/(2*g.tsa.sum())); a['ex']=a.pts-2*a.seg.map(lg)*a.tsa
    VAR.append(a[a.seg!='np'].ex.var())
    g=a.groupby(['shooter','seg']).agg(ex=('ex','sum'),n=('ex','size'),tsa=('tsa','sum')).reset_index(); g['season']=y
    pr=pd.read_parquet(f'{D}/out/poss_{y}.parquet',columns=['game_id','deff','off_lu','lq']); pp=pd.read_parquet(f'{D}/out_po/poss_{y}.parquet',columns=['off_lu','lq'])
    pr=pr[pr.lq==1]; pp=pp[pp.lq==1]
    pr['seg']=np.where(pr.deff.isin(pot),'pl',np.where(pr.game_id%2==1,'npA','npB')); pp['seg']='po'
    U=[]
    for P in (pr,pp):
        n=P.off_lu.map(len).to_numpy(); U.append(pd.DataFrame({'shooter':np.concatenate(P.off_lu.to_numpy()),'seg':np.repeat(P.seg.to_numpy(),n)}).groupby(['shooter','seg']).size().rename('poss').reset_index())
    U=pd.concat(U); Unp=U[U.seg.isin(['npA','npB'])].groupby('shooter').poss.sum().reset_index().assign(seg='np'); U=pd.concat([U,Unp])
    rows.append(g.merge(U,on=['shooter','seg'],how='left'))
G=pd.concat(rows); s2=np.mean(VAR); print('per-attempt sd of points above league efficiency',round(np.sqrt(s2),3))
G['pa']=100*G.ex/G.poss; G['v']=1e4*G.n*s2/G.poss**2
W=G.pivot_table(index=['shooter','season'],columns='seg',values=['pa','v','tsa','poss']); W.columns=[a+'_'+b for a,b in W.columns]; W=W.reset_index()
W=W[(W.tsa_po>=50)&(W.tsa_pl>=100)&(W.tsa_npA>=50)&(W.tsa_npB>=50)&(W.poss_po>=300)].copy()
nm=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','name']].drop_duplicates('shooter').set_index('shooter').name; W['name']=W.shooter.map(nm)
W['gap']=W.pa_po-W.pa_pl; W['vg']=W.v_po+W.v_pl
W['gap_pl']=W.pa_pl-W.pa_npA; W['vgp']=W.v_pl+W.v_npA
def fit(y,x,v):
    X=np.column_stack([np.ones(len(W)),W[x]]); w=1/W[v].to_numpy(); sw=np.sqrt(w); b=np.linalg.lstsq(X*sw[:,None],W[y].to_numpy()*sw,rcond=None)[0]; return b,W[y].to_numpy()-X@b
b,W['r']=fit('gap','pa_np','vg'); bp,W['rp']=fit('gap_pl','pa_npB','vgp')
print(f'playoffs: league-wide gap {b[0]:+.2f} pts per 100 poss, regression slope {b[1]:+.3f};  placebo: {bp[0]:+.2f}, {bp[1]:+.3f};  player-seasons {len(W)}')
def persist(col,v):
    p=W.groupby('shooter').apply(lambda g: pd.Series({'m':np.average(g[col],weights=1/g[v]),'v':1/np.sum(1/g[v]),'k':len(g),'name':g['name'].iloc[0]})); p=p[p.k>=4]
    m_=p.m.astype(float); v_=p.v.astype(float)
    def Qt(t2): w=1/(v_+t2); mb=np.sum(w*m_)/w.sum(); return np.sum(w*(m_-mb)**2)
    Q=Qt(0); df=len(p)-1; w=1/v_; tau2=max(0,(Q-df)/(w.sum()-np.sum(w**2)/w.sum())); up=next((t for t in np.arange(0,10,0.05) if Qt(t*t)<chi2.ppf(.025,df)),np.nan)
    d=W[W.shooter.isin(p.index)].copy(); d['k']=d.groupby('shooter').season.rank()
    od=d[d.k%2==1].groupby('shooter').apply(lambda g: np.average(g[col],weights=1/g[v])); ev=d[d.k%2==0].groupby('shooter').apply(lambda g: np.average(g[col],weights=1/g[v])); j=od.index.intersection(ev.index)
    d=d.sort_values(['shooter','season']); d['nx']=d.groupby('shooter')[col].shift(-1); x=d.dropna(subset=['nx'])
    return p.assign(m=m_,se=np.sqrt(v_)),tau2,Q,df,up,np.corrcoef(od[j],ev[j])[0,1],x[col].corr(x.nx)
for lab,c,v in [('PLAYOFFS','r','vg'),('PLACEBO','rp','vgp')]:
    p,tau2,Q,df,up,r,rc=persist(c,v)
    print(f'{lab}: players {len(p)}, Q {Q:.0f}/{df} (p={1-chi2.cdf(Q,df):.3f}), persistent SD {np.sqrt(tau2):.2f} pts/100 (95% upper {up:.2f}), split-half r {r:.3f}, consecutive r {rc:.3f}')
    if lab=='PLAYOFFS': P=p; t2=tau2
P['shrunk']=P.m*t2/(t2+P.v.astype(float)); P=P.sort_values('m').reset_index(); P['rank']=P.index+1
P.round(3).to_csv(f'{OUT}/po_value_players.csv',index=False); W.round(3).to_csv(f'{OUT}/po_value_seasons.csv',index=False)
c=['name','k','m','se','shrunk','rank']
print('most negative'); print(P.sort_values('shrunk')[c].head(10).round(2).to_string(index=False))
print('most positive'); print(P.sort_values('shrunk')[c].tail(12).round(2).to_string(index=False))
ch=['James Harden','Chris Paul','LeBron James','Karl Malone','John Stockton','Reggie Miller','Kawhi Leonard','Dirk Nowitzki','Nikola Jokic','Kobe Bryant','Stephen Curry','Kevin Durant','Giannis Antetokounmpo','Jimmy Butler','Luka Doncic','Tim Duncan','Shaquille O\'Neal','Dwyane Wade','Russell Westbrook','Paul George','Damian Lillard','Joel Embiid','Jayson Tatum']
print(P[P.name.isin(ch)][c].sort_values('m').round(2).to_string(index=False)); print('of',len(P))
