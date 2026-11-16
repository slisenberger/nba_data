"""REAVES tables: shooting fouls drawn above xFoul, valued in points.
  REAVES_shot  = sum (fouled - xF) * (V_F - V_N)   [immediate: league value of a fouled vs unfouled attempt, zone/season]
  REAVES       = REAVES_shot + (fouls - xF) * D_season, where D_season is the average downstream value of a shooting
                 foul that season from foul_epa.py (bonus possessions + whistle slope + opponent foul trouble)."""
import pandas as pd, numpy as np, glob, os
SUF=os.environ.get('SUF','')
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
names=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'{D}/out/names_*.parquet'))]).drop_duplicates('pid').set_index('pid').name
X=pd.read_parquet(f'{D}/xfoul_att{SUF}.parquet',columns=['game_id','season','shooter','w','f_w','xf_w','reaves_shot','is3','loc_logged','kind'])
X['fm_logged']=X.w*X.loc_logged*(X.kind=='fouled_miss'); X['fm']=X.w*(X.kind=='fouled_miss')
# own-FT% version: value each foul above expected at the shooter's own FT% that season instead of the league's
import json as _j
_par={int(k):v for k,v in _j.load(open(f'{D}/xfoul_par{SUF}.json')).items()}
_z=pd.read_parquet(f'{D}/xfoul_att{SUF}.parquet',columns=['z','xf','fouled','dv'])
X['z']=_z.z.values; X['xf']=_z.xf.values; X['fouled']=_z.fouled.values; X['dv']=_z.dv.values
X['psi']=[_par[s_]['psi'].get(z_,0.2) for s_,z_ in zip(X.season,X.z)]
_ft=pd.concat([pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['shooter','kind','fta','ftm']).assign(season=y) for y in sorted(X.season.unique())])
_ft=_ft[_ft.kind.isin(['and1','fouled_miss'])]
_lg=_ft.groupby('season').ftm.sum()/_ft.groupby('season').fta.sum()
_ps=_ft.groupby(['shooter','season'])[['fta','ftm']].sum()
_ps['ftp']=(_ps.ftm+20*_lg.reindex(_ps.index.get_level_values(1)).to_numpy())/(_ps.fta+20)   # shrunk with 20 pseudo-FTA
X['ftp_own']=_ps.ftp.reindex(pd.MultiIndex.from_arrays([X.shooter,X.season])).to_numpy()
X['ftp_lg']=X.season.map(_lg)
X['pv']=np.where(X.is3==1,3,2)
X['reaves_shot_own']=X.reaves_shot+(X.fouled-X.xf)*X.w*(X.ftp_own-X.ftp_lg)*(X.psi+(1-X.psi)*X.pv)
# ---- size-adjusted xF: an average shooter of the same build (heights.py). Height and weight are collinear (r ~ .8;
# audit 2), so they are combined into one size index = first principal component of standardized height and weight.
# Per zone, the residual (fouled - xF) is regressed on the centred size index (attempt-weighted); xF_size = xF + fitted.
# Slope uncertainty: player bootstrap (200 draws) on player x zone cells -> per-player intervals for the adjustment.
_H=pd.read_csv(f'{D}/ext/heights.csv')[['shooter','height_in','weight']].dropna()
_zh=(_H.height_in-_H.height_in.mean())/_H.height_in.std(); _zw=(_H.weight-_H.weight.mean())/_H.weight.std()
_H['size']=(_zh+_zw)/np.sqrt(2)          # PC1 of two standardized variables with positive correlation
X=X.merge(_H[['shooter','size']],on='shooter',how='left')
X['xf_size']=X.xf; SZ=[]
_cells=X[X['size'].notna()].assign(res=lambda d:(d.fouled-d.xf)*d.w).groupby(['shooter','z']).agg(att=('w','sum'),res=('res','sum'),size=('size','first')).reset_index()
BOOT={}
_rng=np.random.default_rng(7); _pls=_cells.shooter.unique()
for z_,g in _cells.groupby('z'):
    m_=np.average(g['size'],weights=g.att); sc=g['size']-m_
    b_=np.sum(sc*g.res)/np.sum(g.att*sc**2)       # residual per attempt per unit of size
    bs=[]
    gi=g.set_index('shooter')
    for _ in range(200):
        dr=_rng.choice(_pls,len(_pls)); gg=gi.reindex(dr).dropna()
        sc2=gg['size']-np.average(gg['size'],weights=gg.att); bs.append(np.sum(sc2*gg.res)/np.sum(gg.att*sc2**2))
    BOOT[z_]=(b_,np.std(bs),m_)
    idx=X.index[(X.z==z_)&X['size'].notna()]
    X.loc[idx,'xf_size']=X.loc[idx,'xf']+b_*(X.loc[idx,'size']-m_)
    SZ.append(dict(zone=z_,per_sd_size_per100=100*b_,se=100*np.std(bs)))
pd.DataFrame(SZ).round(4).to_csv(f'{OUT}/size_adjustment{SUF}.csv',index=False); print('size adjustment by zone (fouls per 100 att per SD of size)',pd.DataFrame(SZ).round(3).to_dict('records'))
# per-player SE of the size adjustment from the slope SEs (zones treated as independent)
X['size_adj_var']=(X.w*(X['size']-X.z.map({k:v[2] for k,v in BOOT.items()})))
X['xfs_w']=X.xf_size*X.w
X['var_r']=(X.xf*(1-X.xf))*(X.w*X.dv)**2   # binomial variance of (fouled - xF) * dv
X['reaves_shot_size']=(X.fouled-X.xf_size)*X.w*X.dv
X['reaves_shot_size_own']=X.reaves_shot_size+(X.fouled-X.xf_size)*X.w*(X.ftp_own-X.ftp_lg)*(X.psi+(1-X.psi)*X.pv)
FE=pd.read_parquet(f'{D}/foul_epa.parquet',columns=['season','shoot','bonus','slope','trouble'])
Dn=FE[FE.shoot==1].assign(d=lambda t: t.bonus+t.slope+t.trouble).groupby('season').d.mean()
S=X.groupby(['shooter','season']).agg(var_r=('var_r','sum'),xf_size=('xfs_w','sum'),reaves_shot_size=('reaves_shot_size','sum'),reaves_shot_size_own=('reaves_shot_size_own','sum'),reaves_shot_own=('reaves_shot_own','sum'),fm=('fm','sum'),fm_logged=('fm_logged','sum'),att=('w','sum'),fouls=('f_w','sum'),xf=('xf_w','sum'),reaves_shot=('reaves_shot','sum'),gp=('game_id','nunique')).reset_index()
S['downstream']=(S.fouls-S.xf)*S.season.map(Dn)
S['reaves']=S.reaves_shot+S.downstream
S['reaves_own_ft']=S.reaves_shot_own+S.downstream
S['downstream_size']=(S.fouls-S.xf_size)*S.season.map(Dn)
S['reaves_size']=S.reaves_shot_size+S.downstream_size; S['reaves_size_own_ft']=S.reaves_shot_size_own+S.downstream_size
# on-court offensive possessions (cf_players) for a per-possession rate
_op=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'{D}/cf/cf_players_*.parquet'))])[['pid','season','off_poss']]
S=S.merge(_op.rename(columns={'pid':'shooter'}),on=['shooter','season'],how='left')
S['reaves_per100poss']=100*S.reaves/S.off_poss; S['reaves_size_per100']=100*S.reaves_size/S.att; S['reaves_size_per100poss']=100*S.reaves_size/S.off_poss; S['reaves_own_ft_per100poss']=100*S.reaves_own_ft/S.off_poss; S['att_per100poss']=100*S.att/S.off_poss
S['foe']=S.fouls/S.xf; S['name']=S.shooter.map(names)
S['reaves_per100']=100*S.reaves/S.att; S['reaves_per_g']=S.reaves/S.gp
S.round(4).to_csv(f'{OUT}/reaves_seasons{SUF}.csv',index=False)
C=S.groupby('shooter').agg(var_r=('var_r','sum'),xf_size=('xf_size','sum'),reaves_size=('reaves_size','sum'),reaves_size_own_ft=('reaves_size_own_ft','sum'),reaves_own_ft=('reaves_own_ft','sum'),off_poss=('off_poss','sum'),first=('season','min'),last=('season','max'),gp=('gp','sum'),att=('att','sum'),fouls=('fouls','sum'),xf=('xf','sum'),reaves_shot=('reaves_shot','sum'),downstream=('downstream','sum'),reaves=('reaves','sum')).reset_index()
C['name']=C.shooter.map(names); C['foe']=C.fouls/C.xf; C['extra_fouls']=C.fouls-C.xf
C['reaves_per100']=100*C.reaves/C.att; C['reaves_per100_se']=100*np.sqrt(C.var_r)/C.att; C['reaves_size_per100']=100*C.reaves_size/C.att; C['reaves_size_per100poss']=100*C.reaves_size/C.off_poss; C['reaves_size_own_ft_per100poss']=100*C.reaves_size_own_ft/C.off_poss; C['reaves_per100poss']=100*C.reaves/C.off_poss; C['reaves_own_ft_per100poss']=100*C.reaves_own_ft/C.off_poss; C['att_per100poss']=100*C.att/C.off_poss; C['reaves_per_g']=C.reaves/C.gp
_za=X[X['size'].notna()].groupby(['shooter','z']).agg(k=('size_adj_var','sum'),dv=('dv','mean'))
_za['v']=[ (k*BOOT[z][1]*dv)**2 for (sh,z),k,dv in zip(_za.index,_za.k,_za.dv)]
C['size_adj_se_per100']=100*np.sqrt(_za.groupby(level=0).v.sum()).reindex(C.shooter).to_numpy()/C.att
C=C[C.att>=3000]
C.round(4).to_csv(f'{OUT}/reaves_career{SUF}.csv',index=False)
pd.set_option('display.width',250)
print('downstream value per shooting foul by season',Dn.round(3).to_dict())
cols=['name','first','last','gp','att','fouls','xf','foe','extra_fouls','reaves_shot','reaves','reaves_per100','reaves_per_g']
print(C.sort_values('reaves',ascending=False)[cols].head(30).round(2).to_string())
print(C.sort_values('reaves_per100',ascending=False)[cols].head(20).round(2).to_string())
print(C.sort_values('reaves')[cols].head(12).round(2).to_string())
SS=S[S.att>=500]
print(SS.sort_values('reaves',ascending=False)[['name','season','gp','att','fouls','xf','foe','reaves','reaves_per_g']].head(25).round(2).to_string())
# ---- top 10 per 100 attempts in recent seasons (min 600 attempts), plus career per 100 (min 3,000)
T=[]
for s_ in range(2019,2026):
    t=S[(S.season==s_)&(S.att>=600)].sort_values('reaves_per100',ascending=False).head(10).copy(); t['rank']=range(1,len(t)+1); T.append(t)
T=pd.concat(T)[['season','rank','name','gp','att','fouls','xf','foe','reaves','reaves_per100','reaves_per100poss','reaves_own_ft']]
T.round(2).to_csv(f'{OUT}/reaves_top10_recent{SUF}.csv',index=False)
print(T[T.season>=2021].round(1).to_string(index=False))

# ---- per 100 possessions views
cc=['name','first','last','off_poss','att_per100poss','foe','reaves','reaves_per100','reaves_per100poss','reaves_own_ft_per100poss']
print(C.sort_values('reaves_per100poss',ascending=False)[cc].head(20).round(2).to_string())
T2=[]
for s_ in range(2019,2026):
    t=S[(S.season==s_)&(S.off_poss>=1500)].sort_values('reaves_per100poss',ascending=False).head(10).copy(); t['rank']=range(1,len(t)+1); T2.append(t)
T2=pd.concat(T2)[['season','rank','name','gp','off_poss','att','foe','reaves','reaves_per100','reaves_per100poss','reaves_own_ft_per100poss','reaves_size_per100','reaves_size_per100poss']]
T2.round(2).to_csv(f'{OUT}/reaves_top10_recent_per100poss{SUF}.csv',index=False)

cs=['name','first','last','reaves_per100','reaves_size_per100','reaves_per100poss','reaves_size_per100poss','reaves_size_own_ft_per100poss']
print(C.sort_values('reaves_size_per100',ascending=False)[cs].head(15).round(2).to_string())
