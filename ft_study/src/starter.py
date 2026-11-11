import pandas as pd, numpy as np
from collections import Counter, defaultdict
arrs=[]; teams=[]
for y in range(1996,2025):
    P=pd.read_parquet(f'/home/claude/data/out/poss_{y}.parquet',columns=['off','deff','off_lu','def_lu'])
    cnt=defaultdict(Counter)
    for off,d,a,b in zip(P.off,P.deff,P.off_lu,P.def_lu):
        for x in a: cnt[off][x]+=1
        for x in b: cnt[d][x]+=1
    top={t:set(p for p,_ in c.most_common(5)) for t,c in cnt.items()}
    no=np.array([sum(1 for x in a if x in top[o]) for o,a in zip(P.off,P.off_lu)],dtype=np.int8)
    nd=np.array([sum(1 for x in b if x in top[d]) for d,b in zip(P.deff,P.def_lu)],dtype=np.int8)
    arrs.append(pd.DataFrame({'n5o':no,'n5d':nd}))
    pt=defaultdict(Counter)
    for t,c in cnt.items():
        for p,v in c.items(): pt[p][t]+=v
    for p,c in pt.items():
        t,v=c.most_common(1)[0]; teams.append(dict(pid=int(p),season=y,team=int(t),poss_both=sum(c.values())))
    print(y,flush=True)
pd.concat(arrs,ignore_index=True).to_parquet('/home/claude/data/top5.parquet')
pd.DataFrame(teams).to_parquet('/home/claude/data/player_team.parquet')
