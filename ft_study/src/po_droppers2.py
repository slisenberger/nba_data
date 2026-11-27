"""Playoff over/under-performance in relative scoring efficiency, with league-wide effects and candidate explanations
controlled, then a persistence test on what is left.
Per player-season (1996-97 to 2024-25): relative TS (vs that segment's league) in RS games vs non-playoff teams (np),
RS games vs playoff teams (pl), playoffs (po).
Model (WLS, weights 1/sampling variance, SEs clustered by player):
  rTS_po = a + b1 rTS_pl + b2 rTS_np + covariates
The intercept and slopes absorb the league-wide playoff drop, the stronger playoff field and regression to the mean.
Covariates ("other stuff"): age, height, RS usage (TS attempts per 100 on-court possessions), playoff usage change, minutes-load
change (on-court possessions per game, PO vs RS), RS 3PA share, RS FT rate (FTA/FGA), RS shooting-foul ratio (fouls/xF),
playoff opponents' regular-season defensive rating (vs league), season.
Persistence: random-effects SD of player means of the residual, split-half and consecutive-run correlations; placebo with the
same model and RS-vs-playoff-teams as the target (no playoffs).
Outputs results/po_droppers2_*.csv"""
import pandas as pd, numpy as np, re, glob
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'; SD1=0.575
rows=[]; usg=[]; opp=[]
for y in range(1996,2025):
    po=pd.read_parquet(f'{D}/out_po/att_{y}.parquet',columns=['off','shooter','kind','pts','fta','is3']); pot=set(po.off.unique())
    rs=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['deff','shooter','kind','pts','fta','is3','game_id'])
    rs['seg']=np.where(rs.deff.isin(pot),'pl','np'); po['seg']='po'
    rs2=rs[rs.seg=='np'].copy(); rs2['seg']=np.where(rs2.game_id%2==1,'npA','npB')
    a=pd.concat([rs.drop(columns=['deff','game_id']),rs2.drop(columns=['deff','game_id']),po.drop(columns='off')]); a=a[a.shooter>0]
    a['fga']=a.kind.isin(['fga','and1']).astype(float); a['tsa']=a.fga+0.44*a.fta; a['f3']=a.fga*a.is3
    lg=a.groupby('seg').apply(lambda g: g.pts.sum()/(2*g.tsa.sum()))
    g=a.groupby(['shooter','seg']).agg(pts=('pts','sum'),tsa=('tsa','sum'),fga=('fga','sum'),fta=('fta','sum'),f3=('f3','sum')).reset_index(); g['lg']=g.seg.map(lg); g['season']=y; rows.append(g)
    # on-court possessions and games, opponents' defensive rating
    pr=pd.read_parquet(f'{D}/out/poss_{y}.parquet',columns=['game_id','off','deff','off_lu','lq','pts']); pp=pd.read_parquet(f'{D}/out_po/poss_{y}.parquet',columns=['game_id','off','deff','off_lu','lq','pts'])
    lgppp=pr.pts.mean(); drtg=(pr.groupby('deff').pts.mean()-lgppp)*100
    pr['seg']=np.where(pr.deff.isin(pot),'pl','np'); pp['seg']='po'
    for P in (pr,pp):
        P=P[P.lq==1]; n=P.off_lu.map(len).to_numpy()
        v=pd.DataFrame({'pid':np.concatenate(P.off_lu.to_numpy()),'seg':np.repeat(P.seg.to_numpy(),n),'g':np.repeat(P.game_id.to_numpy(),n),'od':np.repeat(P.deff.map(drtg).to_numpy(),n)})
        u=v.groupby(['pid','seg']).agg(poss=('g','size'),games=('g','nunique'),opp_drtg=('od','mean')).reset_index(); u['season']=y; usg.append(u)
G=pd.concat(rows); U=pd.concat(usg).rename(columns={'pid':'shooter'})
G=G.merge(U,on=['shooter','season','seg'],how='left')
W=G.pivot_table(index=['shooter','season'],columns='seg',values=['pts','tsa','fga','fta','f3','lg','poss','games','opp_drtg']); W.columns=[a+'_'+b for a,b in W.columns]; W=W.reset_index()
for s in ['np','pl','po','npA','npB']: W['rts_'+s]=100*(W['pts_'+s]/(2*W['tsa_'+s])-W['lg_'+s])
W=W[(W.tsa_po>=50)&(W.tsa_pl>=100)&(W.tsa_np>=150)&(W.tsa_npA>=50)&(W.tsa_npB>=50)&(W.poss_po>0)].copy()
rs_tsa=W.tsa_np+W.tsa_pl; rs_poss=W.poss_np+W.poss_pl
W['usage_rs']=100*rs_tsa/rs_poss; W['usage_chg']=100*W.tsa_po/W.poss_po-W.usage_rs
W['load_chg']=W.poss_po/W.games_po-rs_poss/(W.games_np+W.games_pl)
W['three_sh']=(W.f3_np+W.f3_pl)/(W.fga_np+W.fga_pl); W['ft_rate']=(W.fta_np+W.fta_pl)/(W.fga_np+W.fga_pl)
W['opp_d']=W.opp_drtg_po
R=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','season','foe','name']]; W=W.merge(R,on=['shooter','season'],how='left')
H=pd.read_csv(f'{D}/ext/heights.csv')[['shooter','name','height_in']]
bd=[]
for f in glob.glob(f'{D}/ext/bbref/*.html'):
    s=open(f,encoding='utf8',errors='ignore').read()
    for tr in re.findall(r'<tr[^>]*>(.*?)</tr>',s,flags=re.S):
        n=re.search(r'data-stat="player"[^>]*>(?:<strong>)?<a[^>]*>([^<]+)</a>',tr); b=re.search(r'data-stat="birth_date" csk="(\d{8})"',tr); y0=re.search(r'data-stat="year_min"\s*>(\d{4})',tr)
        if n and b and y0: bd.append((n.group(1),int(b.group(1)[:4]),int(b.group(1)[4:6]),int(y0.group(1))))
B=pd.DataFrame(bd,columns=['name','by','bm','ymin'])
B=B[B.ymin>=1975].drop_duplicates('name',keep=False)       # unambiguous names only
H=H.merge(B[['name','by','bm']],on='name',how='left')
W=W.merge(H[['shooter','height_in','by','bm']],on='shooter',how='left')
W['age']=W.season-W.by+np.where(W.bm>=2,0,1)-0.5          # age on Feb 1 of the season
W['gap']=W.rts_po-W.rts_pl; W['v_gap']=(100*SD1)**2*(1/W.tsa_po+1/W.tsa_pl)
W['gap_pl']=W.rts_pl-W.rts_npA; W['v_gpl']=(100*SD1)**2*(1/W.tsa_pl+1/W.tsa_npA)     # placebo: no playoffs anywhere
cov=['age','height_in','usage_rs','usage_chg','load_chg','three_sh','ft_rate','foe','opp_d','season']
M=W.dropna(subset=cov+['gap','rts_np','rts_npB']).copy()
print('player-seasons with all covariates',len(M),'of',len(W),flush=True)
def wls(y,X,w,cl):
    Xm=np.column_stack([np.ones(len(X)),X]); sw=np.sqrt(w); b=np.linalg.lstsq(Xm*sw[:,None],y*sw,rcond=None)[0]
    e=y-Xm@b; XtWX=np.linalg.inv((Xm*w[:,None]).T@Xm); sc=Xm*(w*e)[:,None]
    S=pd.DataFrame(sc).groupby(cl).sum().to_numpy(); V=XtWX@(S.T@S)@XtWX; return b,np.sqrt(np.diag(V)),e
Z=M[cov].copy(); zs=Z.std(); Z=(Z-Z.mean())/zs
def fit(target,vcol,talent,covs=True):
    X=np.column_stack([M[talent]]+([Z[c] for c in cov] if covs else []))
    return wls(M[target].to_numpy(),X,1/M[vcol].to_numpy(),M.shooter.to_numpy())
b0,s0,e0=fit('gap','v_gap','rts_np',covs=False)
b1,s1,e1=fit('gap','v_gap','rts_np')
names=['intercept (league-wide playoff gap)','RS rTS vs other teams (talent)']+[f'{c} (per SD = {zs[c]:.2f})' for c in cov]
T=pd.DataFrame({'term':names,'coef':b1,'se':s1,'z':b1/s1}); print('MODEL: playoff gap = playoff rTS - RS rTS vs playoff teams (TS points)'); print(T.round(3).to_string(index=False)); T.round(4).to_csv(f'{OUT}/po_droppers2_model.csv',index=False)
print('no covariates: intercept',round(b0[0],2),'+-',round(s0[0],2),' talent slope',round(b0[1],3),'+-',round(s0[1],3))
M['resid']=e1
bp,sp_,ep=fit('gap_pl','v_gpl','rts_npB'); M['resid_pl']=ep
print('placebo model: intercept',round(bp[0],2),' talent slope',round(bp[1],3))
def persist(col,vcol):
    p=M.groupby('shooter').apply(lambda g: pd.Series({'m':np.average(g[col],weights=1/g[vcol]),'v':1/np.sum(1/g[vcol]),'k':len(g),'name':g['name'].iloc[0]}))
    p=p[p.k>=4]; m_=p.m.astype(float); v_=p.v.astype(float); w=1/v_; mb=np.sum(w*m_)/w.sum(); Q=np.sum(w*(m_-mb)**2); df=len(p)-1
    tau2=max(0,(Q-df)/(w.sum()-np.sum(w**2)/w.sum()))
    d=M[M.shooter.isin(p.index)].copy(); d['r']=d.groupby('shooter').season.rank()
    od=d[d.r%2==1].groupby('shooter').apply(lambda g: np.average(g[col],weights=1/g[vcol])); ev=d[d.r%2==0].groupby('shooter').apply(lambda g: np.average(g[col],weights=1/g[vcol]))
    j=od.index.intersection(ev.index); r=np.corrcoef(od[j],ev[j])[0,1]
    d=d.sort_values(['shooter','season']); d['nx']=d.groupby('shooter')[col].shift(-1); x=d.dropna(subset=['nx']); rc=x[col].corr(x.nx)
    return p,tau2,Q,df,r,rc
from scipy.stats import chi2
for lab,c,v in [('PLAYOFFS residual','resid','v_gap'),('PLACEBO residual (RS halves only)','resid_pl','v_gpl')]:
    p,tau2,Q,df,r,rc=persist(c,v)
    w=1/p.v.astype(float); m_=p.m.astype(float)
    def Qt(t2): ww=1/(p.v.astype(float)+t2); mb=np.sum(ww*m_)/ww.sum(); return np.sum(ww*(m_-mb)**2)
    up=next((t for t in np.arange(0,6,0.05) if Qt(t*t)<chi2.ppf(.025,df)),None)
    print(f'{lab}: players {len(p)}, Q {Q:.0f} on {df} df (p={1-chi2.cdf(Q,df):.3f}), persistent SD {np.sqrt(tau2):.2f} TS pts (95% upper {up:.2f}), split-half r {r:.3f}, consecutive-run r {rc:.3f}')
    if c=='resid': P=p; tau2_po=tau2
P['shrunk']=P.m.astype(float)*tau2_po/(tau2_po+P.v.astype(float)) if tau2_po>0 else 0.0; P['se']=np.sqrt(P.v.astype(float))
P.sort_values('m').round(3).to_csv(f'{OUT}/po_droppers2_players.csv')
c=['name','k','m','se','shrunk']; print('most negative'); print(P.sort_values('shrunk' if tau2_po>0 else 'm')[c].head(8).round(2).to_string()); print('most positive'); print(P.sort_values('shrunk' if tau2_po>0 else 'm')[c].tail(8).round(2).to_string())
M.round(3).to_csv(f'{OUT}/po_droppers2_seasons.csv',index=False)
