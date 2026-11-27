"""Playoff change in scoring efficiency, decomposed: shot diet (expected field-goal points per FGA from the league's
regular-season make rate by zone x shot type x 2/3 x season) vs shot-making (actual - expected FG points per FGA), plus the
free-throw side (FT trips per FGA, FT%). Segments as po_droppers2: playoffs vs RS games vs that season's playoff teams.
Persistence of each component across playoff runs (random effects + split-half), placebo on RS halves. 1996-97 to 2024-25."""
import pandas as pd, numpy as np
from scipy.stats import chi2
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
zmap={'Restricted Area':'RA','In The Paint (Non-RA)':'PAINT','Mid-Range':'MID','Left Corner 3':'C3','Right Corner 3':'C3','Above the Break 3':'AB3','Backcourt':'BC'}
def stype(a):
    a=a.fillna('').str.lower()
    return np.select([a.str.contains('dunk'),a.str.contains('alley'),a.str.contains('tip|putback'),a.str.contains('layup|finger'),a.str.contains('float'),a.str.contains('hook'),
                      a.str.contains('step back'),a.str.contains('pullup|pull-up'),a.str.contains('fadeaway|turnaround'),a.str.contains('jump')],
                     ['dunk','alley','tip','layup','floater','hook','stepback','pullup','fade','jumper'],'other')
rows=[]; VAR=[]
for y in range(1996,2025):
    po=pd.read_parquet(f'{D}/out_po/att_{y}.parquet',columns=['game_id','evnum','off','shooter','kind','made','is3','pts','fta','ftm']); pot=set(po.off.unique())
    rs=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','evnum','deff','shooter','kind','made','is3','pts','fta','ftm'])
    rs['seg']=np.where(rs.deff.isin(pot),'pl',np.where(rs.game_id%2==1,'npA','npB')); po['seg']='po'
    sr=pd.read_parquet(f'{D}/sd/sd_{y}.parquet',columns=['game_id','evnum','zone','sd_action']); sp=pd.read_parquet(f'{D}/sd/sd_po_{y}.parquet',columns=['game_id','evnum','zone','sd_action'])
    rs=rs.merge(sr,on=['game_id','evnum'],how='left'); po=po.merge(sp,on=['game_id','evnum'],how='left')
    a=pd.concat([rs.drop(columns='deff'),po.drop(columns='off')],ignore_index=True); a=a[a.shooter>0]
    f=a[a.kind.isin(['fga','and1'])].copy(); f['z']=f.zone.astype(object).map(zmap).fillna('UNK'); f['t']=stype(f.sd_action.astype(object))
    f['fgp']=f.made*(2+f.is3)
    cell=f[f.seg!='po'].groupby(['z','t','is3']).fgp.mean(); cz=f[f.seg!='po'].groupby(['z','is3']).fgp.mean()
    e=cell.reindex(pd.MultiIndex.from_arrays([f.z,f.t,f.is3])).to_numpy(); e2=cz.reindex(pd.MultiIndex.from_arrays([f.z,f.is3])).to_numpy()
    f['xfgp']=np.where(np.isnan(e),e2,e)
    lgx=f.groupby('seg').apply(lambda g: pd.Series({'lx':g.xfgp.mean(),'lm':(g.fgp-g.xfgp).mean()}))
    VAR.append(((f.fgp-f.xfgp).var()*2500, f.groupby('shooter').xfgp.transform(lambda v: v-v.mean()).var()*2500))
    g=f.groupby(['shooter','seg']).agg(fga=('fgp','size'),fgp=('fgp','sum'),xfgp=('xfgp','sum')).reset_index()
    t=a[a.fta>0].groupby(['shooter','seg']).agg(fta=('fta','sum'),ftm=('ftm','sum')).reset_index()
    g=g.merge(t,on=['shooter','seg'],how='left').fillna({'fta':0,'ftm':0}); g=g.join(lgx,on='seg'); g['season']=y; rows.append(g)
G=pd.concat(rows)
G['diet']=100*(G.xfgp/G.fga-G.lx)/2          # TS-point scale (FG pts per FGA / 2), relative to segment league
G['make']=100*((G.fgp-G.xfgp)/G.fga-G.lm)/2
W=G.pivot_table(index=['shooter','season'],columns='seg',values=['fga','diet','make','fta','ftm']); W.columns=[a+'_'+b for a,b in W.columns]; W=W.reset_index()
W=W[(W.fga_po>=40)&(W.fga_pl>=80)&(W.fga_npA>=40)&(W.fga_npB>=40)].copy()
nm=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','name']].drop_duplicates('shooter').set_index('shooter').name; W['name']=W.shooter.map(nm)
VM,VD=np.mean([v[0] for v in VAR]),np.mean([v[1] for v in VAR])   # per-FGA variance (TS-pt^2): make residual, shot quality within player
print('per-FGA sd, TS points: make residual',round(np.sqrt(VM),1),' shot quality within player',round(np.sqrt(VD),1))
for comp in ['diet','make']:
    W[comp+'_gap']=W[comp+'_po']-W[comp+'_pl']; W[comp+'_gpl']=W[comp+'_pl']-W[comp+'_npA']
for comp,VV in [('make',VM),('diet',VD)]:
    W[f'v_po_{comp}']=VV*(1/W.fga_po+1/W.fga_pl); W[f'v_pl_{comp}']=VV*(1/W.fga_pl+1/W.fga_npA)
print('player-seasons',len(W))
print('league-wide (FGA-weighted) playoff gap: diet',round(np.average(W.diet_gap,weights=W.fga_po),2),' make',round(np.average(W.make_gap,weights=W.fga_po),2))
def persist(col,v):
    d=W.copy(); d['c']=d[col]-np.average(d[col],weights=1/d[v])
    p=d.groupby('shooter').apply(lambda g: pd.Series({'m':np.average(g.c,weights=1/g[v]),'v':1/np.sum(1/g[v]),'k':len(g)})); p=p[p.k>=4]
    def Qt(t2): w=1/(p.v+t2); mb=np.sum(w*p.m)/w.sum(); return np.sum(w*(p.m-mb)**2)
    Q=Qt(0); df=len(p)-1; w=1/p.v; tau2=max(0,(Q-df)/(w.sum()-np.sum(w**2)/w.sum())); up=next((t for t in np.arange(0,8,0.05) if Qt(t*t)<chi2.ppf(.025,df)),np.nan)
    d=d[d.shooter.isin(p.index)]; d['r']=d.groupby('shooter').season.rank()
    od=d[d.r%2==1].groupby('shooter').apply(lambda g: np.average(g.c,weights=1/g[v])); ev=d[d.r%2==0].groupby('shooter').apply(lambda g: np.average(g.c,weights=1/g[v])); j=od.index.intersection(ev.index)
    return p,tau2,Q,df,up,np.corrcoef(od[j],ev[j])[0,1]
res={}
for comp in ['diet','make']:
    for lab,col,v in [('playoffs',comp+'_gap',f'v_po_{comp}'),('placebo',comp+'_gpl',f'v_pl_{comp}')]:
        p,tau2,Q,df,up,r=persist(col,v); res[(comp,lab)]=(p,tau2)
        print(f'{comp:5s} {lab:8s}: players {len(p)}, Q {Q:.0f}/{df} (p={1-chi2.cdf(Q,df):.3f}), persistent SD {np.sqrt(tau2):.2f} (95% upper {up:.2f}), split-half r {r:.3f}')
# diet has little sampling noise of the binomial kind; its SE above is conservative. Report the diet shift directly too.
h=W[W.name=='James Harden'][['season','fga_po','diet_pl','diet_po','diet_gap','make_pl','make_po','make_gap']]
print('Harden by season (TS points; diet = shot quality, make = shot-making over expected)'); print(h.round(2).to_string(index=False))
hw=W[W.name=='James Harden']; print('Harden FGA-weighted: diet gap',round(np.average(hw.diet_gap,weights=hw.fga_po),2),' make gap',round(np.average(hw.make_gap,weights=hw.fga_po),2))
for comp in ['diet','make']:
    p,tau2=res[(comp,'playoffs')]; p=p.join(W.groupby('shooter').name.first()); p['shrunk']=p.m*tau2/(tau2+p.v); p['se']=np.sqrt(p.v)
    p.sort_values('shrunk').round(3).to_csv(f'{OUT}/po_decomp_{comp}_players.csv')
    print(comp,'most negative'); print(p.sort_values('shrunk')[['name','k','m','se','shrunk']].head(6).round(2).to_string(index=False))
    print(comp,'most positive'); print(p.sort_values('shrunk')[['name','k','m','se','shrunk']].tail(6).round(2).to_string(index=False))
    if 'James Harden' in set(p.name): print(' Harden',p[p.name=='James Harden'][['k','m','se','shrunk']].round(2).to_dict('records'))
W.round(3).to_csv(f'{OUT}/po_decomp_seasons.csv',index=False)
