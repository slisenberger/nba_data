"""Shooting-foul locations from the richer feeds (proposed: exact REAVES).
  2016-17..2024-25: data.nba.com feed (datanba_<y>): foul events have locX/locY on the shot-detail grid, keyed by
                    GAME_ID + evt (= nbastats EVENTNUM), plus the calling official (epid) and FTA (opt3).
  2025-26:          NBA live feed (cdnnba_2025): foul rows have area / areaDetail (no x/y) and officialId, keyed by
                    gameId + actionNumber (= the v3 actionNumber parse.py uses as EVENTNUM).
Output data/floc/floc_<y>.parquet: game_id, evnum (the FOUL's event number), fz (zone of the foul spot), fdist (ft),
official, fta."""
import pandas as pd, numpy as np, sys, os, subprocess
y=int(sys.argv[1]); D='/home/claude/data'
def zone_xy(x,yy):
    # shot-detail zone geometry (tenths of feet, hoop at 0,0): RA 4 ft arc, paint 16 ft wide to 19 ft from baseline
    # (hoop 5.25 ft off the baseline -> y < 137.5), corner 3 |x| >= 220 below y 92.5, arc 23.75 ft
    d=np.sqrt(x**2+yy**2)/10
    return np.select([(d<=4.0),(np.abs(x)<80)&(yy<137.5),(np.abs(x)>=220)&(yy<=92.5),(d>=23.75)],['RA','PAINT','C3','AB3'],'MID'),d
tmp=f'{D}/tmp_floc_{y}'; os.makedirs(tmp,exist_ok=True)
if y<=2024:
    subprocess.run(['tar','-xJf',f'/home/claude/shufinskiy/nba_data/datasets/datanba_{y}.tar.xz','-C',tmp],check=True)
    f=[a for a in os.listdir(tmp) if a.endswith('.csv')][0]
    x=pd.read_csv(f'{tmp}/{f}',low_memory=False,usecols=lambda c: c in ['GAME_ID','evt','etype','mtype','locX','locY','epid','opt3'])
    for c in ('epid','opt3'):
        if c not in x: x[c]=0
    x=x[(x.etype==6)&x.mtype.isin([2,29])]
    z,dd=zone_xy(x.locX.to_numpy(float),x.locY.to_numpy(float))
    out=pd.DataFrame({'game_id':x.GAME_ID.astype('int64'),'evnum':x.evt.astype('int64'),'fz':z,'fdist':dd,'official':x.epid.fillna(0).astype('int64'),'fta':x.opt3.fillna(0).astype(int),'src':'datanba'})
    out.loc[(x.locX.to_numpy()==0)&(x.locY.to_numpy()==0),'fz']=None
else:
    subprocess.run(['tar','-xJf',f'/home/claude/shufinskiy/nba_data/datasets/cdnnba_{y}.tar.xz','-C',tmp],check=True)
    f=[a for a in os.listdir(tmp) if a.endswith('.csv')][0]
    x=pd.read_csv(f'{tmp}/{f}',low_memory=False,usecols=['gameId','actionNumber','actionType','descriptor','qualifiers','area','areaDetail','officialId'])
    x=x[(x.actionType=='foul')&(x.descriptor=='shooting')]
    zm={'Restricted Area':'RA','In The Paint (Non-RA)':'PAINT','Mid-Range':'MID','Left Corner 3':'C3','Right Corner 3':'C3','Above the Break 3':'AB3','Backcourt':'AB3'}
    band=x.areaDetail.astype(str).str.extract(r'^(\d+)')[0].astype(float)+4
    out=pd.DataFrame({'game_id':x.gameId.astype('int64'),'evnum':x.actionNumber.astype('int64'),'fz':x.area.map(zm),'fdist':band,'official':x.officialId.fillna(0).astype('int64'),
                      'fta':pd.to_numeric(x.qualifiers.astype(str).str.extract(r'(\d)freethrow')[0],errors='coerce').fillna(0).astype(int),'src':'cdnnba'})
subprocess.run(['rm','-rf',tmp])
out.to_parquet(f'{D}/floc/floc_{y}.parquet'); print(y,len(out),'zone known',round(out.fz.notna().mean(),4),out.fz.value_counts(normalize=True).round(3).to_dict(),flush=True)
