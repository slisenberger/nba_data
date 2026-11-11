"""Corrected threshold / placebo design. parse.py stamps def_tf at possession start, AFTER any defensive foul at
the start clock (e.g. a loose-ball foul on the rebound that sends the new offense to the line); that foul is
then not counted in the possession (d_shoot_fouls / d_nonshoot_fouls) although its FT points are. So possessions
at a higher team-foul count mechanically contain FT trips. Fix: tf_adj = def_tf - fouls at the start clock
(drawer = offense), and add those fouls back to the possession's foul flags. Then the 0->1 ... 3->4 steps with the
original FEs and with game x defense FE."""
import pandas as pd, numpy as np, gc, glob
import fe
from fe import feols
_dm=fe.demean
fe.demean=lambda M,codes,tol=1e-6,maxit=300,w=None: _dm(M,codes,tol=1e-6,maxit=300,w=w)
D='/home/claude/data'
need=['game_id','period','off','deff','home_off','start_clk','margin','def_tf','start_type','pts','pts_fg','pts_ft_shoot','pts_ft_bonus','pts_ft_other','tov','d_shoot_fouls','d_nonshoot_fouls','lq','season']
P=pd.read_parquet(f'{D}/poss_core.parquet',columns=need)
F=pd.concat([pd.read_parquet(f,columns=['game_id','period','clk','drawer_team','action']) for f in sorted(glob.glob(f'{D}/out/fouls_*.parquet'))])
F['sh']=F.action.isin([2,29]).astype(int)
st0=F.groupby(['game_id','period','drawer_team','clk']).agg(n0=('sh','size'),sh0=('sh','sum')).reset_index()
P=P.merge(st0,left_on=['game_id','period','off','start_clk'],right_on=['game_id','period','drawer_team','clk'],how='left').drop(columns=['drawer_team','clk'])
P[['n0','sh0']]=P[['n0','sh0']].fillna(0)
P['tf_adj']=P.def_tf-P.n0
print('possessions starting with a defensive foul at the start clock',round((P.n0>0).mean(),4),flush=True)
S=P[(P.period<=4)&(P.start_clk>120)&(P.margin.abs()<=20)&(P.lq==1)].reset_index(drop=True); del P,F; gc.collect()
S['tbin']=S.period*100+(S.start_clk//60).astype(int); S['mbin']=(S.margin.clip(-20,20)//3).astype(int)
S['ots']=S.off*10000+S.season; S['dts']=S.deff*10000+S.season; S['gd']=S.game_id.astype(np.int64)*100+S.deff%100
S['any_foul']=((S.d_shoot_fouls+S.d_nonshoot_fouls+S.n0)>0).astype(int)
S['shoot_foul']=((S.d_shoot_fouls+S.sh0)>0).astype(int)
S['pts_nofoul']=np.where(S.any_foul==0,S.pts,np.nan)
S['pts_ft']=S.pts_ft_shoot+S.pts_ft_bonus+S.pts_ft_other
st=pd.get_dummies(S.start_type,prefix='st',drop_first=True).astype(float); S=pd.concat([S,st],axis=1)
ctrl=['home_off']+list(st.columns); res=[]
outs=['pts','pts_fg','pts_ft','pts_ft_bonus','shoot_foul','any_foul','tov']
for tfc in ['def_tf','tf_adj']:
    for a,b in [(0,1),(1,2),(2,3),(3,4)]:
        d=S[S[tfc].isin([a,b])].copy(); d['tr']=(d[tfc]==b).astype(int)
        for fl,fes in {'orig':['tbin','mbin','ots','dts'],'game_def':['tbin','mbin','gd']}.items():
            o=feols(d,outs,['tr']+ctrl,fes,'game_id')
            dn=d[d.pts_nofoul.notna()]; o.update(feols(dn,['pts_nofoul'],['tr']+ctrl,fes,'game_id'))
            for y,(B,SE) in o.items(): res.append(dict(coding=tfc,step=f'{a}->{b}',fe=fl,outcome=y,coef=B[0],se=SE[0],base=d.loc[d.tr==0,y].mean(),n=len(d)))
            print(tfc,a,b,fl,{y:round(o[y][0][0],4) for y in ['pts','pts_fg','pts_ft','pts_nofoul']},flush=True)
            gc.collect()
R=pd.DataFrame(res); R['t']=R.coef/R.se
R.to_csv('/home/user/nba_data/ft_study/results/res_bonus_fix.csv',index=False)
