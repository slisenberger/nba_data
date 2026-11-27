"""Robust playoff riser/dropper analysis (scoring value with turnovers).
Value per segment = 100 x (points - L x (TS attempts + turnovers)) / on-court possessions, L = that segment's league points per
(TS attempt + turnover). Segments per season: RS vs playoff teams (pl), RS vs other teams (np; odd/even halves npA/npB),
playoffs (po), playoffs first round only (po1). Each season vs its own regular season.
Gap = V_po - V_pl, minus a + b V_np (league-wide drop + regression). Player-specific sampling variance from the per-unit
variance of (points - L x units). Placebo: V_pl - V_npA with V_npB as talent.
Reported: persistence (random effects, split-half), equal-run vs precision weighting, first round only, out-of-sample
(estimates from playoffs through 2018-19 predicting 2019-20..2024-25), reputation lists, and the self-creation hypothesis
(RS share of pull-up / step-back / fadeaway / turnaround shots and shot-making over expected on them, 2005+ shot types)."""
import pandas as pd, numpy as np
from scipy.stats import chi2
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
SELF=['pullup','stepback','fade']
def stype(a):
    a=a.fillna('').str.lower()
    return np.select([a.str.contains('dunk|alley|tip|putback|layup|finger|hook|float'),a.str.contains('step back'),a.str.contains('pullup|pull-up'),a.str.contains('fadeaway|turnaround')],
                     ['inside','stepback','pullup','fade'],'other')
zmap={'Restricted Area':'RA','In The Paint (Non-RA)':'PAINT','Mid-Range':'MID','Left Corner 3':'C3','Right Corner 3':'C3','Above the Break 3':'AB3','Backcourt':'BC'}
rows=[]; selfc=[]
for y in range(1996,2025):
    po=pd.read_parquet(f'{D}/out_po/att_{y}.parquet',columns=['game_id','off','shooter','kind','pts','fta']); pot=set(po.off.unique())
    rs=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','evnum','off','deff','shooter','kind','pts','fta','made','is3'])
    opp=pd.concat([rs[['game_id','off','deff']],po.rename(columns={})[['game_id','off']].assign(deff=-1)]).drop_duplicates(['game_id','off'])
    def seg_of(g,deff,is_po):
        if is_po: return np.where((g//100)%10==1,'po1','po2')
        return np.where(pd.Series(deff).isin(pot).to_numpy(),'pl',np.where(g%2==1,'npA','npB'))
    rs['seg']=seg_of(rs.game_id.to_numpy(),rs.deff.to_numpy(),False); po['seg']=seg_of(po.game_id.to_numpy(),None,True)
    a=pd.concat([rs[['game_id','shooter','kind','pts','fta','seg']],po[['game_id','shooter','kind','pts','fta','seg']]]); a=a[a.shooter>0]
    a['u']=a.kind.isin(['fga','and1']).astype(float)+0.44*a.fta
    T1=pd.read_parquet(f'{D}/tov/tov_{y}.parquet'); T2=pd.read_parquet(f'{D}/tov/tov_po_{y}.parquet')
    gt=pd.concat([rs[['game_id','off','deff']].drop_duplicates('game_id'),po[['game_id','off']].drop_duplicates('game_id')])
    T1['seg']=seg_of(T1.game_id.to_numpy(),T1.game_id.map(rs.drop_duplicates(['game_id'])[['game_id','deff']].set_index('game_id').deff).to_numpy(),False)
    # turnover rows: deff for a player = opponent; recompute from player's team in that game
    pt=rs.drop_duplicates(['game_id','shooter'])[['game_id','shooter','deff']].rename(columns={'shooter':'pid'})
    T1=T1.drop(columns='seg').merge(pt,on=['game_id','pid'],how='left'); T1['seg']=seg_of(T1.game_id.to_numpy(),T1.deff.fillna(-1).to_numpy(),False)
    T2['seg']=seg_of(T2.game_id.to_numpy(),None,True)
    TT=pd.concat([T1[['game_id','pid','tov','seg']],T2[['game_id','pid','tov','seg']]]).rename(columns={'pid':'shooter'})
    TT=TT.loc[TT.index.repeat(TT.tov)].assign(kind='tov',pts=0.0,fta=0,u=1.0)[['game_id','shooter','kind','pts','fta','seg','u']]
    a=pd.concat([a,TT],ignore_index=True)
    a['segg']=np.where(a.seg.isin(['po1','po2']),'po',a.seg)
    L=a.groupby('segg').apply(lambda g: g.pts.sum()/g.u.sum()); a['ex']=a.pts-a.segg.map(L)*a.u
    g=a.groupby(['shooter','seg']).agg(ex=('ex','sum'),s2=('ex',lambda v:(v**2).sum()),n=('ex','size'),u=('u','sum'),tsa=('u',lambda v: v[a.loc[v.index,'kind']!='tov'].sum())).reset_index()
    # possessions
    P=[]
    for src,ispo in (('out',False),('out_po',True)):
        p=pd.read_parquet(f'{D}/{src}/poss_{y}.parquet',columns=['game_id','deff','off_lu','lq']); p=p[p.lq==1]
        p['seg']=seg_of(p.game_id.to_numpy(),None if ispo else p.deff.to_numpy(),ispo); n=p.off_lu.map(len).to_numpy()
        P.append(pd.DataFrame({'shooter':np.concatenate(p.off_lu.to_numpy()),'seg':np.repeat(p.seg.to_numpy(),n)}).groupby(['shooter','seg']).size().rename('poss').reset_index())
    g=g.merge(pd.concat(P),on=['shooter','seg'],how='left'); g['season']=y; rows.append(g)
    # self-creation (RS, all segments), shot types from shot detail
    sd=pd.read_parquet(f'{D}/sd/sd_{y}.parquet',columns=['game_id','evnum','zone','sd_action'])
    f=rs[rs.kind.isin(['fga','and1'])].merge(sd,on=['game_id','evnum'],how='left'); f['t']=stype(f.sd_action.astype(object)); f['z']=f.zone.astype(object).map(zmap).fillna('UNK')
    f['fgp']=f.made*(2+f.is3); cell=f.groupby(['z','t','is3']).fgp.mean(); f['x']=cell.reindex(pd.MultiIndex.from_arrays([f.z,f.t,f.is3])).to_numpy()
    f['self']=f.t.isin(SELF)
    sc=f.groupby('shooter').apply(lambda h: pd.Series({'self_share':h.self.mean(),'self_make':((h.fgp-h.x)[h.self]).mean()*50 if h.self.sum()>=30 else np.nan,'self_n':h.self.sum(),'typed':(h.t!='other').mean()}))
    sc['season']=y; selfc.append(sc.reset_index())
    print(y,flush=True)
G=pd.concat(rows); G['po_all']=G.seg.isin(['po1','po2'])
Gp=G[G.po_all].groupby(['shooter','season']).agg(ex=('ex','sum'),s2=('s2','sum'),n=('n','sum'),u=('u','sum'),tsa=('tsa','sum'),poss=('poss','sum')).reset_index().assign(seg='po')
Gn=G[G.seg.isin(['npA','npB'])].groupby(['shooter','season']).agg(ex=('ex','sum'),s2=('s2','sum'),n=('n','sum'),u=('u','sum'),tsa=('tsa','sum'),poss=('poss','sum')).reset_index().assign(seg='np')
G=pd.concat([G.drop(columns='po_all'),Gp,Gn],ignore_index=True)
G['V']=100*G.ex/G.poss; G['var1']=(G.s2/G.n-(G.ex/G.n)**2); G['v']=1e4*G.n*G.var1/G.poss**2
W=G.pivot_table(index=['shooter','season'],columns='seg',values=['V','v','tsa','poss']); W.columns=[a+'_'+b for a,b in W.columns]; W=W.reset_index()
W=W[(W.tsa_po>=50)&(W.tsa_pl>=100)&(W.tsa_npA>=50)&(W.tsa_npB>=50)&(W.poss_po>=300)].copy()
nm=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','name']].drop_duplicates('shooter').set_index('shooter').name; W['name']=W.shooter.map(nm)
def resid(y,x,v,d):
    X=np.column_stack([np.ones(len(d)),d[x]]); w=1/d[v].to_numpy(); sw=np.sqrt(w); b=np.linalg.lstsq(X*sw[:,None],d[y].to_numpy()*sw,rcond=None)[0]; return b,d[y].to_numpy()-X@b
W['gap']=W.V_po-W.V_pl; W['vg']=W.v_po+W.v_pl; b,W['r']=resid('gap','V_np','vg',W)
W['gap_pl']=W.V_pl-W.V_npA; W['vgp']=W.v_pl+W.v_npA; bp,W['rp']=resid('gap_pl','V_npB','vgp',W)
m1=W.tsa_po1>=40; W1=W[m1].copy(); W1['gap1']=W1.V_po1-W1.V_pl; W1['vg1']=W1.v_po1+W1.v_pl; b1,W1['r1']=resid('gap1','V_np','vg1',W1)
print(f'\nplayer-seasons {len(W)}; league-wide playoff gap {b[0]:+.2f} pts/100 poss (first round {b1[0]:+.2f}); placebo {bp[0]:+.2f}')
def summ(d,col,v,k=4,equal=False):
    p=d.groupby('shooter').apply(lambda g: pd.Series({'m':(g[col].mean() if equal else np.average(g[col],weights=1/g[v])),'v':(g[v].sum()/len(g)**2 if equal else 1/np.sum(1/g[v])),'k':len(g),'name':g['name'].iloc[0]}))
    p=p[p.k>=k]; p['m']=p.m.astype(float); p['v']=p.v.astype(float); return p
def persist(p):
    def Qt(t2): w=1/(p.v+t2); mb=np.sum(w*p.m)/w.sum(); return np.sum(w*(p.m-mb)**2)
    Q=Qt(0); df=len(p)-1; w=1/p.v; tau2=max(0,(Q-df)/(w.sum()-np.sum(w**2)/w.sum())); up=next((t for t in np.arange(0,10,0.02) if Qt(t*t)<chi2.ppf(.025,df)),np.nan)
    return Q,df,1-chi2.cdf(Q,df),np.sqrt(tau2),up
print('\nPERSISTENCE (players with >=4 runs): Q/df, p, persistent SD pts/100 poss, 95% upper')
res={}
for lab,d,col,v,eq in [('playoffs, precision-weighted',W,'r','vg',False),('playoffs, equal weight per run',W,'r','vg',True),('first round only',W1,'r1','vg1',False),('PLACEBO (RS halves)',W,'rp','vgp',False)]:
    p=summ(d,col,v,equal=eq); Q,df,pv,tau,up=persist(p); res[lab]=(p,tau); print(f'  {lab:32s} n {len(p)}  Q {Q:.0f}/{df}  p {pv:.3f}  SD {tau:.2f}  upper {up:.2f}')
# rankings (precision-weighted, shrunk)
p,tau=res['playoffs, precision-weighted']; pe,_=res['playoffs, equal weight per run']; p1,_=res['first round only']
p['shrunk']=p.m*tau**2/(tau**2+p.v); p['se']=np.sqrt(p.v); p['equal_w']=pe.m.reindex(p.index); p['first_round']=p1.m.reindex(p.index)
p=p.sort_values('shrunk'); p.round(3).to_csv(f'{OUT}/po_robust_players.csv')
c=['name','k','m','se','shrunk','equal_w','first_round']
print('\nTOP FALLERS'); print(p[c].head(12).round(2).to_string(index=False)); print('\nTOP CLIMBERS'); print(p[c].tail(12).iloc[::-1].round(2).to_string(index=False))
# out-of-sample
tr=W[W.season<=2018]; te=W[W.season>=2019]
ptr=summ(tr,'r','vg',k=3); Q,df,pv,tau_tr,up=persist(ptr); ptr['shr']=ptr.m*tau_tr**2/(tau_tr**2+ptr.v)
pte=summ(te,'r','vg',k=2); j=ptr.index.intersection(pte.index)
x=ptr.shr[j]; yv=pte.m[j]; w=1/pte.v[j]
def wcorr(x,y,w): mx=np.average(x,weights=w); my=np.average(y,weights=w); return np.sum(w*(x-mx)*(y-my))/np.sqrt(np.sum(w*(x-mx)**2)*np.sum(w*(y-my)**2))
rng=np.random.default_rng(5); bs=[]
for _ in range(2000):
    i=rng.integers(0,len(j),len(j)); bs.append(wcorr(x.iloc[i].to_numpy(),yv.iloc[i].to_numpy(),w.iloc[i].to_numpy()))
slope=np.sum(w*(x-np.average(x,weights=w))*(yv-np.average(yv,weights=w)))/np.sum(w*(x-np.average(x,weights=w))**2)
print(f'\nOUT-OF-SAMPLE: estimates from playoffs through 2018-19 (>=3 runs, persistent SD {tau_tr:.2f}) vs 2019-20..2024-25 (>=2 runs): players {len(j)}, weighted corr {wcorr(x,yv,w):.3f} (95% CI {np.percentile(bs,2.5):.3f} to {np.percentile(bs,97.5):.3f}), calibration slope {slope:.2f}')
# placebo OOS
trp=summ(tr,'rp','vgp',k=3); _,_,_,tp,_=persist(trp); trp['shr']=trp.m*tp**2/(tp**2+trp.v); tep=summ(te,'rp','vgp',k=2); jp=trp.index.intersection(tep.index)
print(f'  placebo OOS corr {wcorr(trp.shr[jp],tep.m[jp],1/tep.v[jp]):.3f} (n {len(jp)})')
# reputation lists
rep_drop=['James Harden','John Stockton','Karl Malone']; rep_rise=['Reggie Miller','LeBron James','Kawhi Leonard','Dirk Nowitzki','Nikola Jokic']
allp=summ(W,'r','vg',k=1)
def grp(names,pp):
    q=pp[pp.name.isin(names)]; w=1/q.v; return np.sum(w*q.m)/w.sum(), 1/np.sqrt(w.sum()), list(q.name)
for lab,pp in [('full sample',allp),('out-of-sample period 2019-24 only',summ(te,'r','vg',k=1))]:
    md,sd_,nd=grp(rep_drop,pp); mr,sr,nr=grp(rep_rise,pp)
    print(f'\nREPUTATION ({lab}): droppers {md:+.2f} ± {sd_:.2f} {nd} | risers {mr:+.2f} ± {sr:.2f} {nr} | difference {mr-md:+.2f} ± {np.hypot(sd_,sr):.2f}')
print(allp[allp.name.isin(rep_drop+rep_rise)].assign(se=lambda d: np.sqrt(d.v))[['name','k','m','se']].round(2).sort_values('m').to_string(index=False))
# self-creation hypothesis
SC=pd.concat(selfc); SC=SC[(SC.season>=2005)&(SC.typed>=0.5)]
pc=SC.groupby('shooter').apply(lambda h: pd.Series({'self_share':np.average(h.self_share,weights=h.self_n+1),'self_make':np.average(h.self_make.fillna(0),weights=h.self_n*h.self_make.notna()+1e-9)}))
q=p.join(pc,how='inner'); q=q[q.index.isin(W[W.season>=2005].shooter)]
w=1/(q.v+tau**2)
for c_ in ['self_share','self_make']:
    print(f'\nSELF-CREATION: corr(player playoff effect, RS {c_}) = {wcorr(q.m,q[c_],w):.3f}  (players {len(q)})')
d=W[W.season>=2005].merge(SC[['shooter','season','self_share','self_make']],on=['shooter','season'],how='inner').dropna()
X=np.column_stack([np.ones(len(d)),d.V_np,(d.self_share-d.self_share.mean())/d.self_share.std(),(d.self_make-d.self_make.mean())/d.self_make.std()]); ww=1/d.vg.to_numpy(); sw=np.sqrt(ww)
bb=np.linalg.lstsq(X*sw[:,None],d.gap.to_numpy()*sw,rcond=None)[0]; e=d.gap.to_numpy()-X@bb; XtWX=np.linalg.inv((X*ww[:,None]).T@X); sc_=X*(ww*e)[:,None]; S_=pd.DataFrame(sc_).groupby(d.shooter.to_numpy()).sum().to_numpy(); se=np.sqrt(np.diag(XtWX@(S_.T@S_)@XtWX))
print(f'season-level: gap ~ self-creation share {bb[2]:+.2f} ± {se[2]:.2f} per SD; self-creation shot-making {bb[3]:+.2f} ± {se[3]:.2f} per SD (pts/100 poss; n {len(d)})')
W.round(3).to_csv(f'{OUT}/po_robust_seasons.csv',index=False)
