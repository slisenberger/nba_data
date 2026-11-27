"""Tale-of-two-careers test: for each player with >= 8 playoff runs, the split into two contiguous eras (>= 3 runs each) with the
largest difference in playoff value gap (per game, residual after league-wide drop; po_pergame_seasons.csv). Significance by
permutation of run order (5,000 shuffles), which accounts for choosing the best split. Also reports the team-change split."""
import pandas as pd, numpy as np
OUT='/home/user/nba_data/ft_study/results'; D='/home/claude/data'
W=pd.read_csv(f'{OUT}/po_pergame_seasons.csv')
# team per playoff run (most possessions)
tm=[]
for y in range(1996,2025):
    p=pd.read_parquet(f'{D}/out_po/poss_{y}.parquet',columns=['off','off_lu','lq']); p=p[p.lq==1]; n=p.off_lu.map(len).to_numpy()
    v=pd.DataFrame({'shooter':np.concatenate(p.off_lu.to_numpy()),'t':np.repeat(p.off.to_numpy(),n)}).groupby(['shooter','t']).size().reset_index(name='n').sort_values('n').drop_duplicates('shooter',keep='last')
    tm.append(v.assign(season=y)[['shooter','season','t']])
W=W.merge(pd.concat(tm),on=['shooter','season'],how='left')
rng=np.random.default_rng(11)
def best_split(r,v,min_side=3):
    w=1/v; best=(0,None,0,0)
    for k in range(min_side,len(r)-min_side+1):
        a=np.sum(w[:k]*r[:k])/w[:k].sum(); b=np.sum(w[k:]*r[k:])/w[k:].sum(); se=np.sqrt(1/w[:k].sum()+1/w[k:].sum()); z=(b-a)/se
        if abs(z)>abs(best[0]): best=(z,k,a,b)
    return best
rows=[]
for s,g in W.groupby('shooter'):
    if len(g)<8: continue
    g=g.sort_values('season'); r=g.r.to_numpy(); v=g.vg.to_numpy()
    z,k,a,b=best_split(r,v)
    null=[abs(best_split(r[i],v[i])[0]) for i in (rng.permutation(len(r)) for _ in range(5000))]
    p=np.mean(np.array(null)>=abs(z))
    yrs=g.season.to_numpy(); teams=g.t.to_numpy()
    rows.append(dict(name=g['name'].iloc[0],runs=len(g),split_after=f"{yrs[k-1]}-{str(yrs[k-1]+1)[2:]}",early=a,late=b,diff=b-a,z=z,p_perm=p,early_runs=k,late_runs=len(g)-k))
R=pd.DataFrame(rows).sort_values('p_perm'); R.round(3).to_csv(f'{OUT}/po_eras.csv',index=False)
print('players tested',len(R),'; expected p<0.05 by chance',round(0.05*len(R),1),'; observed',int((R.p_perm<0.05).sum()))
print(R.head(15).round(2).to_string(index=False))
for n in ['Dirk Nowitzki','Chris Paul','James Harden','LeBron James','Kobe Bryant','Tim Duncan','Kevin Durant','Stephen Curry','Russell Westbrook','Paul Pierce','Kevin Garnett','Tony Parker','Manu Ginobili','Dwyane Wade']:
    q=R[R.name==n]
    if len(q): print(q.round(2).to_string(index=False,header=False))
