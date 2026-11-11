"""Shot-attempt accounting from PBP (proposed work item 1).
Shooting attempt (SA) = FGA + fouled miss (2- or 3-FT shooting trip). An and-one is one attempt: a made FGA that was
also fouled. Exact points per shooting attempt and an exact TS%-style rate that counts each non-shooting FT trip as
one attempt (technical FTs excluded), replacing TS%'s 0.44*FTA approximation."""
import pandas as pd, numpy as np, glob, sys
import os
OUT='/home/user/nba_data/ft_study/results'
PO=os.environ.get('PO','')=='1'; TAG='_po' if PO else ''; SRC='out_po' if PO else 'out'
cols=['game_id','season','shooter','kind','made','is3','fouled','ft_trip','fta','ftm','pts']
A=pd.concat([pd.read_parquet(f,columns=cols) for f in sorted(glob.glob(f'/home/claude/data/{SRC}/att_*.parquet'))],ignore_index=True)
names=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob('/home/claude/data/out*/names_*.parquet'))]).drop_duplicates('pid').set_index('pid').name
k=A.kind
A['sa']=k.isin(['fga','and1','fouled_miss']).astype(int)
A['fga']=k.isin(['fga','and1']).astype(int)
A['fg3a']=A.fga*A.is3; A['fgm']=A.made; A['fg3m']=A.made*A.is3
A['and1']=(k=='and1').astype(int); A['fm2']=((k=='fouled_miss')&(A.is3==0)).astype(int); A['fm3']=((k=='fouled_miss')&(A.is3==1)).astype(int)
A['and1_3']=A.and1*A.is3
A['sa3']=A.sa*A.is3
A['fouled3']=A.and1_3+A.fm3
A['sa_pts']=A.pts*A.sa
A['sh_fta']=A.fta*A.sa; A['sh_ftm']=A.ftm*A.sa
A['ns_trips']=(k.isin(['ns_trip','unlinked_trip'])).astype(int)
A['ns_fta']=A.fta*(1-A.sa); A['ns_ftm']=A.ftm*(1-A.sa)   # bonus / away / flagrant / stray FTs
agg=['sa','fga','fgm','fg3a','fg3m','and1','fm2','fm3','and1_3','sa3','fouled3','sa_pts','sh_fta','sh_ftm','ns_trips','ns_fta','ns_ftm','pts']
def stats(G):
    G=G.copy()
    G['fouled']=G.and1+G.fm2+G.fm3
    G['fta']=G.sh_fta+G.ns_fta; G['ftm']=G.sh_ftm+G.ns_ftm
    G['foul_rate']=G.fouled/G.sa                         # share of shooting attempts fouled
    G['and1_rate']=G.and1/G.sa
    G['and1_share']=G.and1/G.fouled.where(G.fouled>0)     # share of shooting fouls converted
    G['foul_rate3']=G.fouled3/G.sa3.where(G.sa3>0)
    G['ppsa']=G.sa_pts/G.sa                               # exact points per shooting attempt
    G['trips_per100']=100*(G.fouled+G.ns_trips)/(G.sa+G.ns_trips)
    G['ts_exact']=G.pts/(2*(G.sa+G.ns_trips))             # every trip = one attempt, techs excluded
    G['ts_classic']=G.pts/(2*(G.fga+0.44*G.fta))
    G['fta_per_trip']=G.fta/(G.fouled+G.ns_trips+(G.ns_fta-G.ns_trips*0).clip(lower=0)*0)  # informational
    G['ftr']=G.fta/G.fga
    G['efg']=(G.fgm+0.5*G.fg3m)/G.fga
    return G
L=stats(A.groupby('season')[agg].sum())
L['implied_ft_coef']=(L.sa+L.ns_trips-L.fga)/L.fta       # what 0.44 should be: attempts per FTA
L.round(4).to_csv(f'{OUT}/shots_league{TAG}.csv')
PS=A[A.shooter>0].groupby(['shooter','season'])[agg].sum().reset_index()
PS=stats(PS); PS['name']=PS.shooter.map(names)
PS.to_parquet(f'/home/claude/data/shots_ps{TAG}.parquet')
C=A[A.shooter>0].groupby('shooter')[agg].sum()
C=C.join(PS.groupby('shooter').season.agg(first='min',last='max'))
C=stats(C).reset_index(); C['name']=C.shooter.map(names)
C=C[C.sa>=(600 if PO else 3000)]
# league-relative versions (season-weighted by the player's attempts)
lg=L[['foul_rate','ppsa','ts_exact','ts_classic']]
PS2=PS.join(lg,on='season',rsuffix='_lg')
for c in ['foul_rate','ppsa','ts_exact','ts_classic']:
    PS2[c+'_xs']=(PS2[c]-PS2[c+'_lg'])*PS2.sa
rel=PS2.groupby('shooter')[[c+'_xs' for c in ['foul_rate','ppsa','ts_exact','ts_classic']]].sum()
C=C.join(rel,on='shooter')
C['extra_fouls']=C.foul_rate_xs                  # fouled attempts above league rate
C['ppsa_rel']=C.ppsa_xs/C.sa; C['ts_exact_rel']=C.ts_exact_xs/C.sa; C['ts_classic_rel']=C.ts_classic_xs/C.sa
C['ts_gap']=C.ts_exact-C.ts_classic
C.to_csv(f'{OUT}/shots_career{TAG}.csv',index=False)
pd.set_option('display.width',250)
print(L[['sa','foul_rate','and1_rate','and1_share','foul_rate3','ppsa','ts_exact','ts_classic','implied_ft_coef']].round(4).to_string())
show=['name','first','last','sa','foul_rate','and1_rate','and1_share','foul_rate3','ppsa','ts_exact','ts_classic','ts_gap','extra_fouls']
print(C.sort_values('foul_rate',ascending=False)[show].head(30).round(3).to_string())
print(C.sort_values('extra_fouls',ascending=False)[show].head(30).round(3).to_string())
print(C.sort_values('ts_gap')[show].head(12).round(3).to_string()); print(C.sort_values('ts_gap')[show].tail(12).round(3).to_string())
