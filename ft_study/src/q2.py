import pandas as pd, numpy as np, glob
F=pd.concat([pd.read_parquet(f,columns=['period','clk','action','pen_before','tf_after','flips_single','flips_single_on','fouler_pf','fouler','season']) for f in sorted(glob.glob('/home/claude/data/cf/cf_fouls_*.parquet'))],ignore_index=True)
P=pd.read_parquet('/home/claude/data/poss_core.parquet',columns=['season','pts_ft_shoot','pts_ft_bonus','pts'])
shoot=F.action.isin([2,29]); bon=(~shoot)&(F.pen_before==1)
agg=pd.DataFrame({'shoot_fouls':F[shoot].groupby('season').size(),'bonus_fouls':F[bon].groupby('season').size(),'nonbonus_ns':F[(~shoot)&(F.pen_before==0)].groupby('season').size()})
pp=P.groupby('season').agg(ft_shoot=('pts_ft_shoot','sum'),ft_bonus=('pts_ft_bonus','sum'),ppp=('pts','mean'))
A=agg.join(pp); A['pts_per_shoot_foul']=A.ft_shoot/A.shoot_fouls; A['pts_per_bonus_foul']=A.ft_bonus/A.bonus_fouls
print(A[['pts_per_shoot_foul','pts_per_bonus_foul','ppp']].round(3).to_string())
print('pooled', (A.ft_shoot.sum()/A.shoot_fouls.sum()).round(3),(A.ft_bonus.sum()/A.bonus_fouls.sum()).round(3))
F['k_before']=F.tf_after-1
F['mbin']=(F.clk//180).clip(upper=3).astype(int)  # 0: last 3 min ... 3: first 3 min
R=F[F.period<=4].groupby(['period','mbin','k_before']).agg(n=('flips_single','size'),flips=('flips_single','mean'),on_share=('flips_single_on','sum')).reset_index()
R['on_share']=R.on_share/(R.flips*R.n)
R=R[R.n>=300]
R.to_csv('/home/claude/data/res_flips_state.csv',index=False)
print(R.pivot_table(index=['period','mbin'],columns='k_before',values='flips').round(2).to_string())
print('overall mean flips per counting foul',F.flips_single.mean().round(3),'on share',(F.flips_single_on.sum()/F.flips_single.sum()).round(3))
