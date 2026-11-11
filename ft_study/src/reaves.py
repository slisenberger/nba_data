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
FE=pd.read_parquet(f'{D}/foul_epa.parquet',columns=['season','shoot','bonus','slope','trouble'])
Dn=FE[FE.shoot==1].assign(d=lambda t: t.bonus+t.slope+t.trouble).groupby('season').d.mean()
S=X.groupby(['shooter','season']).agg(fm=('fm','sum'),fm_logged=('fm_logged','sum'),att=('w','sum'),fouls=('f_w','sum'),xf=('xf_w','sum'),reaves_shot=('reaves_shot','sum'),gp=('game_id','nunique')).reset_index()
S['downstream']=(S.fouls-S.xf)*S.season.map(Dn)
S['reaves']=S.reaves_shot+S.downstream
S['foe']=S.fouls/S.xf; S['name']=S.shooter.map(names)
S['reaves_per100']=100*S.reaves/S.att; S['reaves_per_g']=S.reaves/S.gp
S.round(4).to_csv(f'{OUT}/reaves_seasons{SUF}.csv',index=False)
C=S.groupby('shooter').agg(first=('season','min'),last=('season','max'),gp=('gp','sum'),att=('att','sum'),fouls=('fouls','sum'),xf=('xf','sum'),reaves_shot=('reaves_shot','sum'),downstream=('downstream','sum'),reaves=('reaves','sum')).reset_index()
C['name']=C.shooter.map(names); C['foe']=C.fouls/C.xf; C['extra_fouls']=C.fouls-C.xf
C['reaves_per100']=100*C.reaves/C.att; C['reaves_per_g']=C.reaves/C.gp
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
T=pd.concat(T)[['season','rank','name','gp','att','fouls','xf','foe','reaves','reaves_per100']]
T.round(2).to_csv(f'{OUT}/reaves_top10_recent{SUF}.csv',index=False)
print(T[T.season>=2021].round(1).to_string(index=False))
