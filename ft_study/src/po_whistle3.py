"""Playoff whistle with real playoff xF (xfoul_po.py: RS model applied to playoff attempts).
Per player-season: fouls f and xF in RS games vs non-playoff teams (base, 'np'), RS games vs playoff teams (placebo, 'pl'),
playoffs ('po'). Shooter skill = his base fouls/xF, shrunk (Poisson, EB). Prediction for a segment = xF_segment x shrunk ratio.
  whistle   = actual / predicted   (shot zone, context and shooter held fixed)
  location  = xF per attempt in the segment / xF per attempt in base   (shot-mix shift)
By season, zone, REAVES quintile, player. Outputs results/po3_*.csv."""
import pandas as pd, numpy as np
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
X=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['game_id','season','shooter','z','w','xf','fouled','dv'])
X=X[X.season<=2024]; X['xfw']=X.xf*X.w; X['fw']=X.fouled*X.w; X['dvx']=X.dv*X.xf*X.w; X['dvf']=X.dv*X.fouled*X.w
segs=[]
for y in range(1996,2025):
    pot=set(pd.read_parquet(f'{D}/out_po/att_{y}.parquet',columns=['off']).off.unique())
    r=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','shooter','deff']).drop_duplicates(['game_id','shooter'])
    r['seg']=np.where(r.deff.isin(pot),'pl','np'); segs.append(r[['game_id','shooter','seg']])
X=X.merge(pd.concat(segs),on=['game_id','shooter'],how='inner')
P=pd.read_parquet(f'{D}/xfoul_att_po.parquet',columns=['game_id','season','shooter','z','w','xf','fouled','dv'])
P['xfw']=P.xf*P.w; P['fw']=P.fouled*P.w; P['dvx']=P.dv*P.xf*P.w; P['dvf']=P.dv*P.fouled*P.w; P['seg']='po'
A=pd.concat([X.drop(columns=['game_id']),P.drop(columns=['game_id'])],ignore_index=True); del X,P
agg=dict(n=('w','sum'),xf=('xfw','sum'),f=('fw','sum'),vx=('dvx','sum'),vf=('dvf','sum'))
S=A.groupby(['shooter','season','seg']).agg(**agg).unstack('seg').fillna(0); S.columns=[a+'_'+b for a,b in S.columns]; S=S.reset_index()
S=S[S.xf_np>=5].copy()
# EB shrink of the base ratio: var(true ratio) = var(obs) - mean(1/xf)
r=S.f_np/S.xf_np; vt=max(np.average((r-1)**2,weights=S.xf_np)-np.average(1/S.xf_np,weights=S.xf_np),1e-4); k=1/vt
S['skill']=(S.f_np+k)/(S.xf_np+k); print('EB prior strength k (pseudo expected fouls)',round(k,1),' sd of true ratio',round(np.sqrt(vt),3))
for t in ['pl','po']: S['pred_'+t]=S['xf_'+t]*S.skill
nm=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','name']].drop_duplicates('shooter').set_index('shooter').name
S['name']=S.shooter.map(nm); S.to_csv(f'{OUT}/po3_player_seasons.csv',index=False)
def tot(d,t): return d['f_'+t].sum()/d['pred_'+t].sum(), np.sqrt(d['pred_'+t].sum())/d['pred_'+t].sum()
print('\n1) LEAGUE: actual / predicted (shooter + shot + context held fixed)')
for t,lab in [('pl','RS vs playoff teams (placebo)'),('po','playoffs')]:
    v,se=tot(S,t); print(f'   {lab:32s} {v:.3f} +- {se:.3f}')
print('   location: xF per attempt vs base   placebo',round((S.xf_pl.sum()/S.n_pl.sum())/(S.xf_np.sum()/S.n_np.sum()),3),'  playoffs',round((S.xf_po.sum()/S.n_po.sum())/(S.xf_np.sum()/S.n_np.sum()),3))
# same-player location shift (attempt-weighted by playoff attempts)
s=S[S.n_po>0]; lp=(s.xf_po.sum()/s.n_po.sum()); lb=np.average(s.xf_np/s.n_np,weights=s.n_po); lpl=np.average(np.where(s.n_pl>0,s.xf_pl/np.maximum(s.n_pl,1),lb),weights=s.n_po)
print('   same players, xF per attempt: base',round(lb,4),' placebo',round(lpl,4),' playoffs',round(lp,4),' -> playoff shot-mix effect',round(lp/lb,3))
# by season
rows=[]
for y,g in S.groupby('season'):
    a,sa=tot(g,'po'); b,sb=tot(g,'pl'); rows.append(dict(season=y,playoffs=a,se_po=sa,placebo=b,se_pl=sb,diff=a-b,loc_po=(g.xf_po.sum()/g.n_po.sum())/np.average(g.xf_np/g.n_np,weights=g.n_po+1e-9)))
SE=pd.DataFrame(rows); SE.round(4).to_csv(f'{OUT}/po3_by_season.csv',index=False); print('\n2) BY SEASON'); print(SE.round(3).to_string(index=False))
# by zone (player skill applied per zone)
Z=A.groupby(['shooter','season','seg','z']).agg(**agg).reset_index().merge(S[['shooter','season','skill']],on=['shooter','season'])
Z['pred']=Z.xf*Z.skill; ZZ=Z.groupby(['z','seg']).agg(n=('n','sum'),f=('f','sum'),pred=('pred','sum'),xf=('xf','sum')); ZZ['ratio']=ZZ.f/ZZ.pred; ZZ['se']=np.sqrt(ZZ.pred)/ZZ.pred
ZZ['share']=ZZ.n/ZZ.groupby(level='seg').n.transform('sum')
print('\n3) BY ZONE: actual/predicted and share of attempts'); print(ZZ.round(3).to_string()); ZZ.round(4).to_csv(f'{OUT}/po3_by_zone.csv')
# REAVES quintiles
S['q']=pd.qcut(S.skill.rank(method='first'),5,labels=[1,2,3,4,5])
print('\n4) BY REAVES QUINTILE (shrunk base fouls/xF)')
q=[]
for qq,g in S.groupby('q',observed=True):
    a,sa=tot(g,'po'); b,sb=tot(g,'pl'); q.append(dict(q=int(qq),skill=np.average(g.skill,weights=g.xf_po+1e-9),playoffs=a,se_po=sa,placebo=b,se_pl=sb,diff=a-b))
Q=pd.DataFrame(q); print(Q.round(3).to_string(index=False)); Q.round(4).to_csv(f'{OUT}/po3_by_quintile.csv',index=False)
d=(Q.playoffs.iloc[-1]-Q.playoffs.iloc[0])-(Q.placebo.iloc[-1]-Q.placebo.iloc[0]); se=np.sqrt((Q.se_po**2).iloc[[0,-1]].sum()+(Q.se_pl**2).iloc[[0,-1]].sum()); print('   top-minus-bottom, playoffs minus placebo',round(d,3),'+-',round(se,3))
# players
s=S[S.n_po>0]
C=s.groupby('shooter').agg(name=('name','first'),runs=('season','size'),first=('season','min'),last=('season','max'),f_po=('f_po','sum'),pred_po=('pred_po','sum'),xf_po=('xf_po','sum'),n_po=('n_po','sum'),
    f_pl=('f_pl','sum'),pred_pl=('pred_pl','sum'),xf_np=('xf_np','sum'),n_np=('n_np','sum'),skill=('skill','mean'),vf=('vf_po','sum'),vx=('vx_po','sum'))
C['whistle_po']=C.f_po/C.pred_po; C['whistle_pl']=C.f_pl/C.pred_pl; C['gap']=C.whistle_po-C.whistle_pl
C['se']=np.sqrt(1/C.pred_po+1/C.pred_pl)*C.whistle_po; C['z']=C.gap/C.se
C['loc_shift']=(C.xf_po/C.n_po)/(C.xf_np/C.n_np); C['fouls_lost']=C.f_po-C.whistle_pl*C.pred_po
C=C[(C.runs>=5)&(C.pred_po>=80)].sort_values('z'); C.round(3).to_csv(f'{OUT}/po3_players.csv',index=False)
c=['name','first','last','runs','f_po','pred_po','whistle_po','whistle_pl','gap','se','z','loc_shift','fouls_lost']
print('\n5) PLAYERS (>=5 runs): falloff'); print(C[c].head(15).round(2).to_string(index=False)); print('gain'); print(C[c].tail(8).round(2).to_string(index=False))
print('\ncase studies'); print(C[C.name.isin(['James Harden','Luka Doncic','Joel Embiid','Jimmy Butler','Allen Iverson','Karl Malone','Dirk Nowitzki','Kawhi Leonard','Jayson Tatum','Kyrie Irving','Shai Gilgeous-Alexander','Giannis Antetokounmpo','LeBron James'])][c].round(2).to_string(index=False))
print('players',len(C),' z<-2:',(C.z<-2).sum(),' z>2:',(C.z>2).sum(),' (chance ~',round(.023*len(C),1),'each)')
