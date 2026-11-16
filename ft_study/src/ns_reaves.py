"""Non-shooting REAVES ("off-ball" REAVES) and combined REAVES.
Every non-shooting, non-offensive counting foul a player draws on offense (reach-ins, holds, loose-ball / rebounding,
away-from-play, inbound) except fouls the defense chose (take, hack-a, late intentional: foul_epa 'skill'==0).
  value of a drawn foul  = its foul EPA (immediate: FTs in the bonus or a side-out; + Shapley bonus credit + trouble)
  expected per possession = LPM with FEs (bonus state x period, period x minute, margin bin, defense team-season) of the
                            possession's total non-shooting-foul EPA drawn by the offense; each of the 5 on-court
                            offensive players is expected to draw 1/5 of it
  nsREAVES = actual - expected (points), per 100 on-court offensive possessions; also in foul counts.
  combined = shooting REAVES + nsREAVES (per 100 possessions).
Seasons 2005-06+ only: before that the PBP doesn't name the fouled player on non-shooting fouls (imputed in cf.py).
Clean-lineup possessions only (lq == 1), for both actual and expected."""
import pandas as pd, numpy as np, glob, gc
from fe import fe_resid
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
FE=pd.read_parquet(f'{D}/foul_epa.parquet',columns=['game_id','period','clk','season','drawer','drawer_team','shoot','skill','epa','action'])
FE=FE[(FE.shoot==0)&(FE.skill==1)&(FE.drawer>0)&(FE.season>=2005)]
rows=[]; prow=[]
for y in range(2005,2026):
    P=pd.read_parquet(f'{D}/out/poss_{y}.parquet',columns=['game_id','period','off','deff','start_clk','end_clk','def_pen','margin','off_lu','lq'])
    P['season']=y; P['pi']=np.arange(len(P))
    f=FE[FE.season==y].copy()
    key=lambda g,p,t: (g.astype(np.int64)*10+p.astype(np.int64))*100+t.astype(np.int64)%100
    P['k']=key(P.game_id,P.period,P.off); f['k']=key(f.game_id,f.period,f.drawer_team)
    a=P[['k','start_clk','pi']].assign(neg=-P.start_clk).sort_values('neg'); b=f.assign(neg=-f.clk).sort_values('neg')
    m=pd.merge_asof(b,a[['neg','k','pi']],on='neg',by='k',direction='backward')
    m=m[m.pi.notna()]; m['pi']=m.pi.astype(int)
    tot=m.groupby('pi').agg(ns_epa=('epa','sum'),ns_n=('epa','size'))
    P=P.join(tot,on='pi'); P[['ns_epa','ns_n']]=P[['ns_epa','ns_n']].fillna(0)
    m['lq']=P.lq.to_numpy()[m.pi.to_numpy()]
    rows.append(m[m.lq==1][['drawer','season','epa']])
    prow.append(P[['season','period','start_clk','def_pen','margin','deff','ns_epa','ns_n','off_lu','lq']])
    print(y,'possessions',len(P),'ns fouls matched',len(m),'of',len(f),flush=True)
P=pd.concat(prow,ignore_index=True); A=pd.concat(rows,ignore_index=True); del prow,rows; gc.collect()
P['f_pen']=P.def_pen*10+P.period.clip(upper=5); P['f_clk']=P.period.clip(upper=5)*100+(P.start_clk//60).astype(int)
P['f_mb']=(P.margin.clip(-24,24)//4).astype(int); P['f_dts']=P.deff.astype(np.int64)%100000*10000+P.season
fes=['f_pen','f_clk','f_mb','f_dts']
P['x_epa']=P.ns_epa-fe_resid(P,'ns_epa',fes); P['x_n']=P.ns_n-fe_resid(P,'ns_n',fes)
print('league: ns-foul EPA per possession',round(P.ns_epa.mean(),4),' ns fouls per possession',round(P.ns_n.mean(),4))
Q=P[P.lq==1]
# expected per player: 1/5 of each on-court possession's expectation
import pyarrow as pa
lu=Q.off_lu.to_numpy()
ex=pd.DataFrame({'pid':np.concatenate(lu),'season':np.repeat(Q.season.to_numpy(),[len(x) for x in lu]),
                 'x_epa':np.repeat(Q.x_epa.to_numpy()/5,[len(x) for x in lu]),'x_n':np.repeat(Q.x_n.to_numpy()/5,[len(x) for x in lu])})
E_=ex.groupby(['pid','season']).agg(poss=('x_epa','size'),x_epa=('x_epa','sum'),x_n=('x_n','sum'))
A_=A.groupby(['drawer','season']).agg(ns_epa=('epa','sum'),ns_n=('epa','size')); A_.index.names=['pid','season']
S=E_.join(A_,how='left').fillna(0).reset_index()
S['ns_reaves']=S.ns_epa-S.x_epa; S['ns_excess_fouls']=S.ns_n-S.x_n
S['ns_reaves_per100poss']=100*S.ns_reaves/S.poss; S['ns_excess_per100poss']=100*S.ns_excess_fouls/S.poss
names=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'{D}/out/names_*.parquet'))]).drop_duplicates('pid').set_index('pid').name
S['name']=S.pid.map(names)
# combine with shooting REAVES (per 100 poss on its own possession base)
R=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','season','reaves','off_poss','reaves_size']].rename(columns={'shooter':'pid'})
S=S.merge(R,on=['pid','season'],how='left')
S['shoot_reaves_per100poss']=100*S.reaves/S.off_poss
S['combined_per100poss']=S.shoot_reaves_per100poss.fillna(0)+S.ns_reaves_per100poss
S.round(4).to_csv(f'{OUT}/ns_reaves_seasons.csv',index=False)
C=S.groupby('pid').agg(name=('name','first'),first=('season','min'),last=('season','max'),poss=('poss','sum'),ns_n=('ns_n','sum'),x_n=('x_n','sum'),
                        ns_epa=('ns_epa','sum'),x_epa=('x_epa','sum'),ns_reaves=('ns_reaves','sum'),reaves=('reaves','sum'),off_poss=('off_poss','sum'))
C=C[C.poss>=15000].copy()
C['ns_ratio']=C.ns_n/C.x_n; C['ns_reaves_per100poss']=100*C.ns_reaves/C.poss
C['shoot_reaves_per100poss']=100*C.reaves/C.off_poss; C['combined_per100poss']=C.shoot_reaves_per100poss+C.ns_reaves_per100poss
C.reset_index().round(4).to_csv(f'{OUT}/ns_reaves_career.csv',index=False)
pd.set_option('display.width',250)
c=['name','first','last','poss','ns_n','x_n','ns_ratio','ns_reaves_per100poss','shoot_reaves_per100poss','combined_per100poss']
print('CAREER (2005-06+), non-shooting REAVES per 100 poss'); print(C.sort_values('ns_reaves_per100poss',ascending=False)[c].head(20).round(2).to_string(index=False))
print('bottom'); print(C.sort_values('ns_reaves_per100poss')[c].head(8).round(2).to_string(index=False))
print('COMBINED'); print(C.sort_values('combined_per100poss',ascending=False)[c].head(20).round(2).to_string(index=False))
