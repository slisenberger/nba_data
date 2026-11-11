"""Event study around pre-bonus team fouls: is scoring already elevated BEFORE the foul (whistle / game-flow
environment) or does it step up only AFTER it (causal)? For every counting defensive foul that leaves the defense
below the bonus (Q1-Q4, >2:00 left, team fouls 1-3 after it), take the defense's possessions -4..+4 around the
possession containing the foul (rel 0), keeping clean events (no other defensive counting foul in the window).
pts ~ rel dummies + event FE (same game, period, defense) + period-minute FE; base = rel -4,-3."""
import pandas as pd, numpy as np, glob, gc
import fe
from fe import feols
_dm=fe.demean
fe.demean=lambda M,codes,tol=1e-6,maxit=300,w=None: _dm(M,codes,tol=1e-6,maxit=300,w=w)
D='/home/claude/data'
P=pd.read_parquet(f'{D}/poss_core.parquet',columns=['game_id','period','off','deff','start_clk','end_clk','def_tf','pts','pts_fg','pts_ft_shoot','pts_ft_bonus','tov','d_shoot_fouls','d_nonshoot_fouls','margin','lq','season','home_off'])
P=P[P.period<=4].reset_index(drop=True)
F=pd.concat([pd.read_parquet(f,columns=['game_id','period','clk','fouler_team']) for f in sorted(glob.glob(f'{D}/out/fouls_*.parquet'))],ignore_index=True)
F=F[F.period<=4]
P['k']=(P.game_id.astype(np.int64)*10+P.period)*100+P.deff%100
F['k']=(F.game_id.astype(np.int64)*10+F.period)*100+F.fouler_team%100
P=P.sort_values(['k','start_clk'],ascending=[True,False]).reset_index(drop=True)
# fouls inside each possession: clk in (end_clk, start_clk]  (a foul at a possession boundary goes to the earlier one)
fk=(F.k.to_numpy()*1000.0+(720-F.clk.to_numpy())); fk.sort()
lo=P.k.to_numpy()*1000.0+(720-P.start_clk.to_numpy()); hi=P.k.to_numpy()*1000.0+(720-P.end_clk.to_numpy())
P['n_in']=np.searchsorted(fk,hi,side='right')-np.searchsorted(fk,lo,side='right')
P['j']=P.groupby('k').cumcount()
ev=P[(P.n_in==1)&(P.def_tf<=2)&(P.end_clk>120)&(P.lq==1)][['k','j']].reset_index()
rows=[]
nin=P.n_in.to_numpy(); kk=P.k.to_numpy(); jj=P.j.to_numpy()
N=len(P)
for e,(i,k,j) in enumerate(ev[['index','k','j']].itertuples(index=False)):
    a,b=i-4,i+4
    if a<0 or b>=N or kk[a]!=k or kk[b]!=k: continue
    if nin[a:b+1].sum()!=1: continue
    for r in range(-4,5): rows.append((i+r,e,r))
S=pd.DataFrame(rows,columns=['pi','ev','rel'])
S=S.join(P[['pts','pts_fg','pts_ft_shoot','pts_ft_bonus','tov','start_clk','period','margin','home_off','game_id']],on='pi')
S['tbin']=S.period*100+(S.start_clk//60).astype(int)
S['mbin']=(S.margin.clip(-20,20)//3).astype(int)
print('clean events',S.ev.nunique(),'rows',len(S),flush=True)
for r in [-2,-1,0,1,2,3,4]: S[f'r{r}']=(S.rel==r).astype(float)
xs=[f'r{r}' for r in [-2,-1,0,1,2,3,4]]+['home_off']
o=feols(S,['pts','pts_fg','pts_ft_shoot','tov'],xs,['ev','tbin','mbin'],'game_id')
out=[]
for y,(B,SE) in o.items():
    for x,b,s in zip(xs,B,SE):
        if x.startswith('r'): out.append(dict(outcome=y,rel=int(x[1:]),coef=b,se=s))
R=pd.DataFrame(out); R.to_csv(f'{D}/res_event.csv',index=False)
print(R.pivot_table(index='rel',columns='outcome',values='coef').round(4).to_string())
print(R.pivot_table(index='rel',columns='outcome',values='se').round(4).to_string())
