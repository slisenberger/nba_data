import pandas as pd, numpy as np, gc
from fe import feols
P=pd.read_parquet('/home/claude/data/poss_core.parquet',columns=['game_id','period','off','deff','home_off','start_clk','margin','pts','lq','season'])
T=pd.read_parquet('/home/claude/data/top5.parquet'); P['n5o']=T.n5o.values; P['n5d']=T.n5d.values; del T
late=(P.period>=4)&(P.start_clk<=120)
S=P[~late&(P.margin.abs()<=20)&(P.start_clk>=24)&(P.lq==1)].reset_index(drop=True); del P; gc.collect()
S['tbin']=S.period.clip(upper=5)*100+(S.start_clk//60).astype(int); S['mbin']=(S.margin.clip(-20,20)//3).astype(int)
S['ots']=S.off*10000+S.season; S['dts']=S.deff*10000+S.season
o=feols(S,['pts'],['n5o','n5d','home_off'],['tbin','mbin','ots','dts'],'game_id')
b,se=o['pts']; print('n5o',b[0],se[0],'n5d',b[1],se[1])
d=(b[0]-b[1])/2; print('delta per player-possession (avg of both ends):',d)
import json; json.dump(dict(n5o=b[0],n5d=b[1],delta=d),open('/home/claude/data/delta.json','w'))
