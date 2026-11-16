"""Pre-specified test: do regular-season EXCESS FOULS lose more value in the playoffs than other scoring skills?
Hypothesis H1: the playoff retention of foul drawing (shooting and non-shooting) is lower than that of shot-making and FT%,
beyond what the same opponents produce in the regular season (placebo).
Components per player-season and segment (A/B = regular-season games vs non-playoff teams, odd/even game ids;
P = regular-season games vs that season's playoff teams; O = playoffs), each relative to the segment's league rate:
  foul  : shooting fouls drawn per shooting attempt minus league rate for the same 2/3 mix
  shot  : FG points per FGA minus league for the same 2/3 mix (shot-making)
  ft    : FT% minus league (control skill, should persist)
  ns    : non-shooting fouls drawn (personal / loose ball / inbound / block; not in the last 2:00 of the 4th or OT)
          per 100 on-court possessions minus league
Retention R = slope(target ~ A) / slope(B ~ A): the playoff (or placebo) slope divided by the split-half slope, which cancels
measurement error in A. Ratio of interest: R_O / R_P per component. Cluster bootstrap by player (400 draws). 2005-06 to 2024-25."""
import pandas as pd, numpy as np
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'; K=['fga','and1','fouled_miss']
NSACT=[1,3,5,27]
recs=[]
for y in range(2005,2025):
    po=pd.read_parquet(f'{D}/out_po/att_{y}.parquet',columns=['off','shooter','kind','is3','made','pts','fta','ftm','game_id'])
    rs=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['off','deff','shooter','kind','is3','made','pts','fta','ftm','game_id'])
    pot=set(po.off.unique())
    rs['seg']=np.where(rs.deff.isin(pot),'P',np.where(rs.game_id%2==1,'A','B')); po['seg']='O'
    A=pd.concat([rs.drop(columns='deff'),po]); A['cls']=np.where(A.seg=='O','po','rs'); A=A[A.shooter>0]
    sh=A[A.kind.isin(K)].copy(); sh['f']=sh.kind.isin(['and1','fouled_miss']).astype(float)
    lr=sh.groupby(['cls','is3']).f.mean(); sh['ef']=lr.reindex(pd.MultiIndex.from_arrays([sh.cls,sh.is3])).to_numpy()
    fg=A[A.kind.isin(['fga','and1'])].copy(); fg['fp']=fg.made*(2+fg.is3)
    lf=fg.groupby(['cls','is3']).fp.mean(); fg['efp']=lf.reindex(pd.MultiIndex.from_arrays([fg.cls,fg.is3])).to_numpy()
    ftr=A[A.fta>0]; lft=ftr.groupby('cls').ftm.sum()/ftr.groupby('cls').fta.sum()
    g1=sh.groupby(['shooter','seg']).agg(n_sh=('f','size'),f=('f','sum'),ef=('ef','sum'))
    g2=fg.groupby(['shooter','seg']).agg(n_fg=('fp','size'),fp=('fp','sum'),efp=('efp','sum'))
    g3=ftr.groupby(['shooter','seg','cls']).agg(fta=('fta','sum'),ftm=('ftm','sum')).reset_index(); g3['eftm']=g3.fta*g3.cls.map(lft); g3=g3.drop(columns='cls').set_index(['shooter','seg'])
    # non-shooting fouls drawn and on-court possessions
    pf=[]; pp=[]
    for src,seg_of in [('out',None),('out_po','O')]:
        F=pd.read_parquet(f'{D}/{src}/fouls_{y}.parquet',columns=['game_id','period','clk','drawer_team','drawn_by','action','shoot'])
        F=F[(F.shoot==0)&F.action.isin(NSACT)&(F.drawn_by>0)&~((F.period>=4)&(F.clk<=120))]
        P=pd.read_parquet(f'{D}/{src}/poss_{y}.parquet',columns=['game_id','off','deff','off_lu','lq']); P=P[P.lq==1]
        if seg_of is None:
            gd=P.drop_duplicates('game_id').set_index('game_id').deff  # not used for team; segment by the opponent of the drawer
            P['seg']=np.where(P.deff.isin(pot),'P',np.where(P.game_id%2==1,'A','B'))
            opp=P.drop_duplicates(['game_id','off']).set_index(['game_id','off']).deff
            od=opp.reindex(pd.MultiIndex.from_arrays([F.game_id,F.drawer_team])).to_numpy()
            F['seg']=np.where(pd.Series(od).isin(pot).to_numpy(),'P',np.where(F.game_id%2==1,'A','B'))
        else: P['seg']='O'; F['seg']='O'
        pf.append(F.groupby(['drawn_by','seg']).size().rename('nsf'))
        v=pd.DataFrame({'pid':np.concatenate(P.off_lu.to_numpy()),'seg':np.repeat(P.seg.to_numpy(),5)}); pp.append(v.groupby(['pid','seg']).size().rename('ns_poss'))
    nsf=pd.concat(pf); nsf.index.names=['shooter','seg']; nsp=pd.concat(pp); nsp.index.names=['shooter','seg']
    g=g1.join(g2,how='outer').join(g3,how='outer').join(nsp,how='outer').join(nsf,how='left').fillna(0).reset_index()
    for cls,segs in [('rs',['A','B','P']),('po',['O'])]:
        m=g.seg.isin(segs); lns=g.loc[m,'nsf'].sum()/g.loc[m,'ns_poss'].sum(); g.loc[m,'ens']=g.loc[m,'ns_poss']*lns
    g['season']=y; recs.append(g); print(y,flush=True)
G=pd.concat(recs); G.to_parquet(f'{D}/po_retention_raw.parquet')
comp={'foul':('f','ef','n_sh',100),'shot':('fp','efp','n_fg',100),'ft':('ftm','eftm','fta',40),'ns':('nsf','ens','ns_poss',800)}
W=G.pivot_table(index=['shooter','season'],columns='seg',values=[c for c in G.columns if c not in ('shooter','seg','season')],fill_value=0)
W.columns=[a+'_'+b for a,b in W.columns]; W=W.reset_index()
def wslope(x,y,w):
    xm=np.average(x,weights=w); ym=np.average(y,weights=w); return np.sum(w*(x-xm)*(y-ym))/np.sum(w*(x-xm)**2)
def retention(Wd):
    out={}
    for c,(a,e,n,mn) in comp.items():
        d=Wd[(Wd[f'{n}_A']>=mn)&(Wd[f'{n}_B']>=mn)]
        xA=(d[f'{a}_A']-d[f'{e}_A'])/d[f'{n}_A']; xB=(d[f'{a}_B']-d[f'{e}_B'])/d[f'{n}_B']
        sAB=wslope(xA,xB,d[f'{n}_B'])
        for t in ['P','O']:
            dt=d[f'{n}_{t}']>=mn/4
            yt=(d[f'{a}_{t}']-d[f'{e}_{t}'])[dt]/d[f'{n}_{t}'][dt]
            out[(c,t)]=wslope(xA[dt],yt,d[f'{n}_{t}'][dt])/sAB
        out[(c,'n')]=int((d[f'{n}_O']>=mn/4).sum())
    return out
est=retention(W)
players=W.shooter.unique(); rng=np.random.default_rng(7); idx=W.groupby('shooter').indices; B=[]
for b in range(400):
    pick=rng.choice(players,len(players)); B.append(retention(W.iloc[np.concatenate([idx[p] for p in pick])]))
rows=[]
for c in comp:
    rp=np.array([x[(c,'P')] for x in B]); ro=np.array([x[(c,'O')] for x in B]); rr=ro/rp
    rows.append(dict(component=c,n_playoff_player_seasons=est[(c,'n')],R_placebo=est[(c,'P')],se_P=rp.std(),R_playoffs=est[(c,'O')],se_O=ro.std(),
                     ratio=est[(c,'O')]/est[(c,'P')],se_ratio=rr.std()))
R=pd.DataFrame(rows); print(R.round(3).to_string(index=False))
dif={}
for c in ['ft','shot','ns']:
    d=np.array([x[('foul','O')]/x[('foul','P')]-x[(c,'O')]/x[(c,'P')] for x in B]); e=est[('foul','O')]/est[('foul','P')]-est[(c,'O')]/est[(c,'P')]
    dif[c]=(e,d.std()); print(f'foul ratio minus {c} ratio: {e:.3f} +- {d.std():.3f}')
d=np.array([x[('ns','O')]/x[('ns','P')]-x[('shot','O')]/x[('shot','P')] for x in B]); print(f"ns ratio minus shot ratio: {est[('ns','O')]/est[('ns','P')]-est[('shot','O')]/est[('shot','P')]:.3f} +- {d.std():.3f}")
R.round(4).to_csv(f'{OUT}/po_retention.csv',index=False)
