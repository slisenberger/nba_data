import pandas as pd, numpy as np, gc
from fe import feols
rows=[]
for y in range(2013,2025):
    P=pd.read_parquet(f'/home/claude/data/out/poss_{y}.parquet',columns=['game_id','period','off','start_clk','margin','def_tf','def_pen','def_lu','def_pf','pts','pts_ft_shoot','pts_ft_bonus','rim_a','rim_m','fga','fgm','fg3m','d_shoot_fouls','d_nonshoot_fouls','tov','lq'])
    P=P[(P.period<=3)&(P.lq==1)&(P.margin.abs()<=20)&(P.start_clk>=24)]
    n=P.def_lu.map(len); P=P[n==5]
    D=np.stack(P.def_lu.to_numpy()); PFc=np.stack(P.def_pf.to_numpy())
    P['pts_nonft']=P.pts-P.pts_ft_shoot-P.pts_ft_bonus; P['efgn']=P.fgm+0.5*P.fg3m
    base=P[['game_id','period','off','start_clk','margin','def_tf','def_pen','pts','pts_nonft','rim_a','rim_m','fga','efgn','d_shoot_fouls','d_nonshoot_fouls','tov']].copy()
    for c in base.columns:
        if c not in ('game_id',): base[c]=pd.to_numeric(base[c],downcast='float' if c in ('start_clk','efgn') else 'integer')
    for k in range(5):
        b=base.copy(); b['pid']=D[:,k]; b['pf']=PFc[:,k]; rows.append(b)
    print(y,flush=True)
X=pd.concat(rows,ignore_index=True); del rows; gc.collect()
X['season']=X.game_id//100000 % 100
X['trouble']=(X.pf>=X.period+1).astype(np.int8)
X['tbin']=(X.period*100+(X.start_clk//60)).astype(np.int32); X=X.drop(columns=['start_clk'])
X['ps']=X.pid*100+X.season; X['gof']=X.game_id.astype(np.int64)*100+(X.off%100)
print(len(X), X.trouble.mean())
X['tfc']=(X.period*10+X.def_tf.clip(upper=6)).astype(np.int16)
outs=['pts','pts_nonft','rim_a','rim_m','fga','efgn','d_shoot_fouls','d_nonshoot_fouls','tov']
o={}
for i in range(0,len(outs),2):
    o.update(feols(X,outs[i:i+2],['trouble','margin','def_pen'],['ps','tbin','gof','tfc'],'game_id')); gc.collect()
R=pd.DataFrame([dict(outcome=k,coef=v[0][0],se=v[1][0],base=X.loc[X.trouble==0,k].mean()) for k,v in o.items()])
# also 'one foul from trouble' placebo: pf == period (e.g. 1 in Q1) vs fewer
X['near']=(X.pf==X.period).astype(np.int8)
Y=X[X.trouble==0]
o2={}
for i in range(0,len(outs),2):
    o2.update(feols(Y,outs[i:i+2],['near','margin','def_pen'],['ps','tbin','gof','tfc'],'game_id')); gc.collect()
R2=pd.DataFrame([dict(outcome=k,coef=v[0][0],se=v[1][0]) for k,v in o2.items()])
R=R.merge(R2,on='outcome',suffixes=('','_near'))
R.to_csv('/home/claude/data/res_q3.csv',index=False); print(R.round(4).to_string())
