"""Foul-neutral shooting efficiency. Shooting points per attempt (FG points + free throws on shooting fouls, per FGA or
fouled miss), relative to the season's league average, on a TS% scale (/2: one attempt ~ one TS attempt).
Foul-neutral version removes the player's shooting REAVES at his own FT% (reaves_shot_own: fouls drawn above expected x
the value of a fouled vs unfouled attempt), i.e. what his attempts would have yielded with an average whistle rate."""
import pandas as pd, numpy as np
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
rows=[]; lg={}
for y in range(1996,2026):
    a=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['shooter','kind','pts','made','is3'])
    a=a[a.kind.isin(['fga','and1','fouled_miss'])&(a.shooter>0)]
    lg[y]=a.pts.sum()/len(a)
    g=a.groupby('shooter').agg(n=('pts','size'),pts=('pts','sum')).reset_index(); g['season']=y; rows.append(g)
G=pd.concat(rows); G['lg']=G.season.map(lg)
R=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','season','name','reaves_shot_own','reaves_shot','foe','off_poss']]
S=G.merge(R,on=['shooter','season'],how='left'); S['reaves_shot_own']=S.reaves_shot_own.fillna(0)
S['rts']=100*(S.pts/S.n-S.lg)/2; S['rts_neutral']=100*((S.pts-S.reaves_shot_own)/S.n-S.lg)/2
S.round(4).to_csv(f'{OUT}/neutral_ts_seasons.csv',index=False)
C=S.groupby('shooter').agg(name=('name','first'),first=('season','min'),last=('season','max'),n=('n','sum'),pts=('pts','sum'),
                            lgpts=('lg',lambda s: (s*S.loc[s.index,'n']).sum()),rs=('reaves_shot_own','sum'),poss=('off_poss','sum')).reset_index()
C['rts']=100*(C.pts-C.lgpts)/C.n/2; C['rts_neutral']=100*(C.pts-C.rs-C.lgpts)/C.n/2; C['foul_part']=C.rts-C.rts_neutral
C['att_per100poss']=100*C.n/C.poss
C=C[C.n>=3000]; C['rk']=C.rts.rank(ascending=False).astype(int); C['rk_neutral']=C.rts_neutral.rank(ascending=False).astype(int)
C.round(3).to_csv(f'{OUT}/neutral_ts_career.csv',index=False)
c=['name','first','last','n','att_per100poss','rts','rk','rts_neutral','rk_neutral','foul_part']
print('players',len(C))
print('TOP neutral'); print(C.sort_values('rts_neutral',ascending=False)[c].head(20).round(2).to_string(index=False))
H=C[(C.n>=8000)&(C.att_per100poss>=25)]
print('HIGH-VOLUME (>=8000 att, >=25 per 100 poss): by neutral'); print(H.sort_values('rts_neutral',ascending=False)[c].head(25).round(2).to_string(index=False))
print('biggest drops'); print(H.sort_values('foul_part',ascending=False)[c].head(8).round(2).to_string(index=False))
print(C[C.name.isin(['Kyrie Irving','James Harden','Stephen Curry','Kevin Durant','Shai Gilgeous-Alexander','Joel Embiid','Luka Doncic','Nikola Jokic'])][c].round(2).to_string(index=False))
