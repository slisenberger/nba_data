import pandas as pd, numpy as np, gc
from fe import feols
need=['game_id','period','off','deff','home_off','start_clk','margin','def_tf','def_pen','start_type','pts','pts_ft_bonus','tov','stl','fga','fgm','fg3m','rim_a','rim_m','d_nonshoot_fouls','lq','season']
P=pd.read_parquet('/home/claude/data/poss_core.parquet',columns=need)
late=(P.period>=4)&(P.start_clk<=120)
S=P[~late & (P.margin.abs()<=20) & (P.start_clk>=24) & (P.lq==1)&(P.period<=4)&(P.start_clk>120)].reset_index(drop=True); del P; gc.collect()
S['tbin']=S.period*100+(S.start_clk//60).astype(int); S['mbin']=(S.margin.clip(-20,20)//3).astype(int)
S['ots']=S.off*10000+S.season; S['dts']=S.deff*10000+S.season
S['efg_num']=S.fgm+0.5*S.fg3m
st=pd.get_dummies(S.start_type,prefix='st',drop_first=True).astype(float); S=pd.concat([S,st],axis=1)
ctrl=['home_off']+list(st.columns); fes=['tbin','mbin','ots','dts']
rows=[]
for a,b in [(1,2),(2,3),(3,4)]:
    d=S[S.def_tf.isin([a,b])].copy(); d['tr']=(d.def_tf==b).astype(int); b0=d[d.tr==0]
    o=feols(d,['efg_num','fga','rim_m','rim_a'],['tr']+ctrl,fes,'game_id')
    e0=b0.efg_num.mean()/b0.fga.mean(); e1=(b0.efg_num.mean()+o['efg_num'][0][0])/(b0.fga.mean()+o['fga'][0][0])
    r0=b0.rim_m.mean()/b0.rim_a.mean(); r1=(b0.rim_m.mean()+o['rim_m'][0][0])/(b0.rim_a.mean()+o['rim_a'][0][0])
    rr0=b0.rim_a.mean()/b0.fga.mean(); rr1=(b0.rim_a.mean()+o['rim_a'][0][0])/(b0.fga.mean()+o['fga'][0][0])
    n=d[d.d_nonshoot_fouls==0]; o2=feols(n,['pts','tov','stl'],['tr']+ctrl,fes,'game_id')
    rows.append(dict(step=f'{a}->{b}',dEFG=e1-e0,dRimFG=r1-r0,dRimShare=rr1-rr0,
        nofoul_dpts=o2['pts'][0][0],nofoul_se=o2['pts'][1][0],nofoul_dtov=o2['tov'][0][0],nofoul_dstl=o2['stl'][0][0],nofoul_base=n[n.tr==0].pts.mean()))
    print(rows[-1],flush=True); gc.collect()
pd.DataFrame(rows).to_csv('/home/claude/data/res_eff.csv',index=False)
