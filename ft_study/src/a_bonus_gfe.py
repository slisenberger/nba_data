"""Priority test from the handoff: is the +0.015 pts/poss per team foul below the bonus causal, or whistle environment?
Re-runs the 3->4 threshold contrast and the 0->1, 1->2, 2->3 placebo steps with (a) the original FEs
(time bin, margin bin, offense-season, defense-season), (b) game x defense FE, (c) game x period x defense FE.
Under (b)/(c) the comparison is between possessions of the same defense in the same game (period) at different
team-foul counts, so a game-level whistle environment cannot drive it."""
import pandas as pd, numpy as np, gc, sys
import fe
from fe import feols
_dm=fe.demean
fe.demean=lambda M,codes,tol=1e-6,maxit=300,w=None: _dm(M,codes,tol=1e-6,maxit=300,w=w)
need=['game_id','period','off','deff','home_off','start_clk','margin','def_tf','def_pen','start_type','pts','pts_fg','pts_ft_shoot','pts_ft_bonus','tov','fga','d_shoot_fouls','d_nonshoot_fouls','lq','season']
P=pd.read_parquet('/home/claude/data/poss_core.parquet',columns=need)
S=P[(P.period<=4)&(P.start_clk>120)&(P.margin.abs()<=20)&(P.lq==1)].reset_index(drop=True); del P; gc.collect()
S['tbin']=S.period*100+(S.start_clk//60).astype(int)
S['mbin']=(S.margin.clip(-20,20)//3).astype(int)
S['ots']=S.off*10000+S.season; S['dts']=S.deff*10000+S.season
S['gd']=S.game_id.astype(np.int64)*100+S.deff%100
S['gpd']=S.gd*10+S.period
S['pts_nobft']=S.pts-S.pts_ft_bonus
S['shoot_foul']=(S.d_shoot_fouls>0).astype(int); S['ns_foul']=(S.d_nonshoot_fouls>0).astype(int)
S['pts_nofoul']=np.where((S.d_shoot_fouls+S.d_nonshoot_fouls)==0,S.pts,np.nan)
st=pd.get_dummies(S.start_type,prefix='st',drop_first=True).astype(float); S=pd.concat([S,st],axis=1)
ctrl=['home_off']+list(st.columns)
outs=['pts','pts_fg','pts_ft_shoot','shoot_foul','ns_foul','tov']
FES={'orig':['tbin','mbin','ots','dts'],'game_def':['tbin','mbin','gd'],'game_period_def':['tbin','mbin','gpd']}
res=[]
STEPS=[tuple(map(int,x.split('-'))) for x in sys.argv[1].split(',')]
for a,b in STEPS:
    d=S[S.def_tf.isin([a,b])].copy(); d['tr']=(d.def_tf==b).astype(int)
    for fl,fes in FES.items():
        o={}
        o.update(feols(d,outs,['tr']+ctrl,fes,'game_id'))
        dn=d[d.pts_nofoul.notna()]
        o.update(feols(dn,['pts_nofoul'],['tr']+ctrl,fes,'game_id'))
        for y,(B,SE) in o.items():
            res.append(dict(step=f'{a}->{b}',fe=fl,outcome=y,coef=B[0],se=SE[0],base=d.loc[d.tr==0,y].mean(),n=len(d)))
        print(a,b,fl,'pts',round(o['pts'][0][0],4),round(o['pts'][1][0],4),'nofoul',round(o['pts_nofoul'][0][0],4),round(o['pts_nofoul'][1][0],4),flush=True)
        gc.collect()
R=pd.DataFrame(res); R['t']=R.coef/R.se
R.to_csv(f'/home/claude/data/res_bonus_gfe_{sys.argv[1]}.csv',index=False)
print(R.pivot_table(index=['outcome','fe'],columns='step',values='coef').round(4).to_string())
print(R.pivot_table(index=['outcome','fe'],columns='step',values='se').round(4).to_string())
