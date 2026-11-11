import pandas as pd, numpy as np
from fe import feols
import gc
need=['game_id','period','off','deff','home_off','start_clk','margin','def_tf','def_pen','start_type','pts','pts_fg','pts_ft_shoot','pts_ft_bonus','tov','stl','fga','fgm','fg3m','rim_a','rim_m','fg3a','oreb','d_shoot_fouls','d_nonshoot_fouls','lq','season']
P=pd.read_parquet('/home/claude/data/poss_core.parquet',columns=need)
late=(P.period>=4)&(P.start_clk<=120)
S=P[~late & (P.margin.abs()<=20) & (P.start_clk>=24) & (P.lq==1)].reset_index(drop=True); del P; gc.collect()
S['tbin']=S.period.clip(upper=5)*100+(S.start_clk//60).astype(int)
S['mbin']=(S.margin.clip(-20,20)//3).astype(int)
S['ots']=S.off*10000+S.season; S['dts']=S.deff*10000+S.season
S['pts_nobft']=S.pts-S.pts_ft_bonus
S['any_ns_foul']=(S.d_nonshoot_fouls>0).astype(int); S['shoot_foul']=(S.d_shoot_fouls>0).astype(int)
st=pd.get_dummies(S.start_type,prefix='st',drop_first=True).astype(float); S=pd.concat([S,st],axis=1)
ctrl=['home_off']+list(st.columns)
outs=['pts','pts_nobft','pts_fg','pts_ft_shoot','pts_ft_bonus','tov','stl','fga','rim_a','fg3a','oreb','shoot_foul','any_ns_foul']
fes=['tbin','mbin','ots','dts']
res=[]
def run(df,label,treat):
    o={}
    for i in range(0,len(outs),3):
        o.update(feols(df,outs[i:i+3],[treat]+ctrl,fes,'game_id')); gc.collect()
    for y in outs:
        res.append(dict(design=label,outcome=y,coef=o[y][0][0],se=o[y][1][0],base=df.loc[df[treat]==0,y].mean(),n=len(df)))
    print(label,'done',flush=True)
run(S,'A_fullFE','def_pen')
T=S[(S.period<=4)&(S.start_clk>120)]
for a,b,l in [(3,4,'B_tf3v4'),(2,3,'C_placebo_tf2v3'),(1,2,'D_placebo_tf1v2')]:
    d=T[T.def_tf.isin([a,b])].copy(); d['tr']=(d.def_tf==b).astype(int); run(d,l,'tr')
R=pd.DataFrame(res); R['t']=R.coef/R.se
R.to_csv('/home/claude/data/res_bonus.csv',index=False)
print(R.round(4).to_string())
# shot-level efficiency in penalty vs not (possession-aggregated, FGA-weighted via ratio of FE-adjusted totals)
sh=S[S.fga>0].copy()
sh['efg_num']=sh.fgm+0.5*sh.fg3m
o=feols(sh,['efg_num','fga'],['def_pen']+ctrl,fes,'game_id'); o.update(feols(sh,['rim_m','rim_a'],['def_pen']+ctrl,fes,'game_id'))
b0=sh[sh.def_pen==0]
e0=b0.efg_num.sum()/b0.fga.sum(); r0=b0.rim_m.sum()/b0.rim_a.sum()
e1=(b0.efg_num.mean()+o['efg_num'][0][0])/(b0.fga.mean()+o['fga'][0][0]); r1=(b0.rim_m.mean()+o['rim_m'][0][0])/(b0.rim_a.mean()+o['rim_a'][0][0])
print(f'eFG base {e0:.4f} -> pen {e1:.4f};  rimFG base {r0:.4f} -> pen {r1:.4f}')
