"""Event study around pre-bonus team fouls: is scoring already elevated BEFORE the foul (whistle / game-flow
environment) or does it step up only AFTER it (causal)? For every counting defensive foul that leaves the defense
below the bonus (Q1-Q4, >2:00 left, team fouls 1-3 after it), take the defense's possessions -4..+4 around the
possession containing the foul (rel 0), keeping clean events (no other defensive counting foul in the window).
pts minus the league mean for that season/period/minute, demeaned within event; base = rel -4,-3; SEs clustered by game."""
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
# fouls inside each possession: clk in [end_clk, start_clk] (a foul at the start clock, e.g. on the rebound, belongs to the new possession)
fk=(F.k.to_numpy()*1000.0+(720-F.clk.to_numpy())); fk.sort()
lo=P.k.to_numpy()*1000.0+(720-P.start_clk.to_numpy())-0.01; hi=P.k.to_numpy()*1000.0+(720-P.end_clk.to_numpy())
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
# expected points by season x period x minute from ALL possessions (league time trend), subtracted out;
# then demean within event. (Minute FE estimated inside the stacked sample are nearly collinear with rel.)
P['tb']=P.season*1000+P.period*100+(P.start_clk//60).astype(int)
ex=P.groupby('tb').pts.mean()
S['season']=P.season.to_numpy()[S.pi.to_numpy()]
S['tb']=S.season*1000+S.period*100+(S.start_clk//60).astype(int)
out=[]
for y in ['pts','pts_fg','pts_ft_shoot','tov']:
    exy=P.groupby('tb')[y].mean()
    r=S[y]-S.tb.map(exy)
    r=r-r.groupby(S.ev).transform('mean')
    S['_r']=r
    base=S.loc[S.rel<=-3,'_r'].mean()
    for rel in range(-4,5):
        m=S.rel==rel
        g=(S.loc[m,'_r']-base).groupby(S.loc[m,'game_id']).agg(['sum','count'])
        est=g['sum'].sum()/g['count'].sum()
        se=np.sqrt(((g['sum']-est*g['count'])**2).sum())/g['count'].sum()
        out.append(dict(outcome=y,rel=rel,coef=est,se=se))
R=pd.DataFrame(out); R.to_csv(f'{D}/res_event.csv',index=False)
R.to_csv('/home/user/nba_data/ft_study/results/res_event.csv',index=False)
print(R.pivot_table(index='rel',columns='outcome',values='coef').round(4).to_string())
print(R.pivot_table(index='rel',columns='outcome',values='se').round(4).to_string())
P2=R[(R.outcome=='pts')]
post=P2[P2.rel.between(1,4)].coef.mean(); pre=P2[P2.rel.between(-2,-1)].coef.mean()
print('pts: mean rel+1..+4',round(post,4),' mean rel-2..-1',round(pre,4))
