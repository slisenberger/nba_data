"""Placebo for the per-team-foul slope: does the OFFENSE's own team-foul count 'raise' its scoring the same way?
Its own fouls give it no free throws, bonus or defensive hesitance, so a similar slope would point to within-game
whistle / flow environment rather than a causal effect of the defense's fouls. Same sample and FEs as
a_bonus_gfe.py (game x period x defense), holding the defense's team-foul count fixed (FE)."""
import pandas as pd, numpy as np, gc, sys
import fe
from fe import feols
_dm=fe.demean
fe.demean=lambda M,codes,tol=1e-6,maxit=300,w=None: _dm(M,codes,tol=1e-6,maxit=300,w=w)
need=['game_id','period','off','deff','home_off','start_clk','margin','def_tf','off_tf','start_type','pts','pts_fg','pts_ft_shoot','tov','d_shoot_fouls','d_nonshoot_fouls','lq','season']
P=pd.read_parquet('/home/claude/data/poss_core.parquet',columns=need)
S=P[(P.period<=4)&(P.start_clk>120)&(P.margin.abs()<=20)&(P.lq==1)&(P.def_tf<=3)].reset_index(drop=True); del P; gc.collect()
S['tbin']=S.period*100+(S.start_clk//60).astype(int); S['mbin']=(S.margin.clip(-20,20)//3).astype(int)
S['gpd']=(S.game_id.astype(np.int64)*100+S.deff%100)*10+S.period
S['tfc']=S.def_tf
st=pd.get_dummies(S.start_type,prefix='st',drop_first=True).astype(float); S=pd.concat([S,st],axis=1)
ctrl=['home_off']+list(st.columns); res=[]
for a,b in [(0,1),(1,2),(2,3)]:
    d=S[S.off_tf.isin([a,b])].copy(); d['tr']=(d.off_tf==b).astype(int)
    for fl,fes in {'game_period_def+def_tf':['tbin','mbin','gpd','tfc']}.items():
        o=feols(d,['pts','pts_fg','pts_ft_shoot','tov'],['tr']+ctrl,fes,'game_id')
        for y,(B,SE) in o.items(): res.append(dict(step=f'off {a}->{b}',fe=fl,outcome=y,coef=B[0],se=SE[0],n=len(d)))
        print(a,b,fl,{y:(round(v[0][0],4),round(v[1][0],4)) for y,v in o.items()},flush=True)
pd.DataFrame(res).to_csv('/home/claude/data/res_offtf_placebo.csv',index=False)
