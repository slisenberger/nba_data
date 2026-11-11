"""Link logged foul locations to shooting attempts and validate them per season.
att (fouled rows) -> fouls (game, period, clk, fouler) -> foul evnum -> floc. Checks: 3-FT trips should be 3PT
zones, 2-FT trips 2PT zones, and-one foul zone vs the shot's own shot-detail zone. Writes data/floc_att.parquet
(att key + foul zone/dist/official) and results/floc_validation.csv."""
import pandas as pd, numpy as np, glob
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
zm={'Restricted Area':'RA','In The Paint (Non-RA)':'PAINT','Mid-Range':'MID','Left Corner 3':'C3','Right Corner 3':'C3','Above the Break 3':'AB3','Backcourt':'AB3'}
rows=[]; links=[]
for y in range(2016,2026):
    L=pd.read_parquet(f'{D}/floc/floc_{y}.parquet')
    F=pd.read_parquet(f'{D}/out/fouls_{y}.parquet',columns=['game_id','period','clk','fouler','evnum'])
    A=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','period','clk','fouler','kind','is3','evnum','ft_trip'])
    A=A[A.kind.isin(['and1','fouled_miss'])].rename(columns={'evnum':'att_evnum'})
    F=F.drop_duplicates(['game_id','period','clk','fouler'])
    m=A.merge(F,on=['game_id','period','clk','fouler'],how='left').merge(L,on=['game_id','evnum'],how='left')
    sd=pd.read_parquet(f'{D}/sd/sd_{y}.parquet',columns=['game_id','evnum','zone']).rename(columns={'evnum':'att_evnum'})
    m=m.merge(sd,on=['game_id','att_evnum'],how='left'); m['sz']=m.zone.astype(object).map(zm)
    # per-game QC: a game whose 2-FT shooting trips land beyond the arc >30% of the time has bad foul coordinates
    t2=m[(m.kind=='fouled_miss')&(m.is3==0)&m.fz.notna()]
    badg=t2.assign(b=t2.fz.isin(['C3','AB3'])).groupby('game_id').b.mean()
    badg=set(badg[badg>0.3].index)
    m['loc_ok']=m.fz.notna()&~m.game_id.isin(badg)
    m['season']=y; links.append(m.drop(columns=['zone']))
    fm=m[m.kind=='fouled_miss']; a1=m[(m.kind=='and1')&m.fz.notna()&m.sz.notna()]
    is3z=m.fz.isin(['C3','AB3'])
    rows.append(dict(season=y,src=L.src.iloc[0],fouled_att=len(m),qc_bad_games=len(badg),loc_ok=round(m.loc_ok.mean(),4),linked=round(m.fz.notna().mean(),4),
        trip3_in_3pt_zone=round(is3z[(m.kind=='fouled_miss')&(m.is3==1)&m.fz.notna()].mean(),3),
        trip2_in_2pt_zone=round((~is3z)[(m.kind=='fouled_miss')&(m.is3==0)&m.fz.notna()].mean(),3),
        and1_zone_match=round((a1.fz==a1.sz).mean(),3),
        and1_rim_bias_mid=round(a1[a1.sz=='MID'].fz.isin(['RA','PAINT']).mean(),3)))
    print(rows[-1],flush=True)
V=pd.DataFrame(rows); V.to_csv(f'{OUT}/floc_validation.csv',index=False)
pd.concat(links,ignore_index=True).to_parquet(f'{D}/floc_att.parquet')
