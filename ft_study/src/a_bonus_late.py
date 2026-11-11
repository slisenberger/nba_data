"""Value of the penalty in the last 2:00 of a quarter (audit: ~half of all bonus possessions are in the last 2:00 / OT,
outside a_bonus_fix.py's sample). Sample: Q1-Q3, 0:24-2:00 left, margin within 20, clean lineups, defense below 4
team fouls (so the penalty comes only from the last-2:00 rule: a defensive foul already committed in the last 2:00).
pts ~ in_penalty + controls | FEs (30-s clock bin x period, margin bin, team fouls, offense-season, defense-season),
and a game x defense FE version. Q4 is left out: end-game intentional fouling."""
import pandas as pd, numpy as np, gc
import fe
from fe import feols
_dm=fe.demean
fe.demean=lambda M,codes,tol=1e-6,maxit=300,w=None: _dm(M,codes,tol=1e-6,maxit=300,w=w)
P=pd.read_parquet('/home/claude/data/poss_core.parquet',columns=['game_id','period','off','deff','home_off','start_clk','margin','def_tf','def_pen','start_type','pts','pts_fg','pts_ft_bonus','tov','lq','season'])
S=P[(P.period<=3)&(P.start_clk<=120)&(P.start_clk>=24)&(P.margin.abs()<=20)&(P.lq==1)&(P.def_tf<=3)].reset_index(drop=True); del P; gc.collect()
S['tbin']=S.period*100+(S.start_clk//30).astype(int); S['mbin']=(S.margin.clip(-20,20)//3).astype(int)
S['ots']=S.off*10000+S.season; S['dts']=S.deff*10000+S.season; S['gd']=S.game_id.astype(np.int64)*100+S.deff%100
st=pd.get_dummies(S.start_type,prefix='st',drop_first=True).astype(float); S=pd.concat([S,st],axis=1)
ctrl=['home_off']+list(st.columns); res=[]
print('n',len(S),'in penalty share',round(S.def_pen.mean(),3))
for fl,fes in {'orig':['tbin','mbin','def_tf','ots','dts'],'game_def':['tbin','mbin','def_tf','gd']}.items():
    o=feols(S,['pts','pts_fg','pts_ft_bonus','tov'],['def_pen']+ctrl,fes,'game_id')
    for y,(B,SE) in o.items(): res.append(dict(fe=fl,outcome=y,coef=B[0],se=SE[0],base=S.loc[S.def_pen==0,y].mean(),n=len(S)))
    print(fl,{y:(round(v[0][0],4),round(v[1][0],4)) for y,v in o.items()},flush=True)
pd.DataFrame(res).to_csv('/home/user/nba_data/ft_study/results/res_bonus_late.csv',index=False)
