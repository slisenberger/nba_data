"""Shapley-style bonus credit (audit fix: the single-foul-removal count in cf.py credits each bonus possession to
every pivotal foul, so credit summed to 1.4-1.6x the actual bonus possessions).
For each opponent possession in the penalty, the fouls that put the team over share it equally: k = team fouls this
period before the possession; if k >= limit each of the k fouls gets 1/k; if only the last-2:00 rule applies, each of
the k2 fouls committed in the last 2:00 gets 1/k2. (For 'at least L of k' games Shapley gives every member 1/k.)
Credit is split by whether the drawer was on the floor for that possession, and by context: 'main' (> 2:00 left in
Q1-Q4) vs 'late' (last 2:00 or OT, valued with its own beta). Output cf/bc_<y>.parquet keyed like cf_fouls."""
import pandas as pd, numpy as np, sys
y=int(sys.argv[1]); D='/home/claude/data'
P=pd.read_parquet(f'{D}/out/poss_{y}.parquet',columns=['game_id','period','off','deff','start_clk','off_lu'])
F=pd.read_parquet(f'{D}/cf/cf_fouls_{y}.parquet',columns=['game_id','period','clk','fouler_team','drawer'])
F['fi']=np.arange(len(F))
cr=np.zeros((len(F),4))  # main_on, main_off, late_on, late_off
Fg=F.groupby(['game_id','period','fouler_team'])
for (g,p,d),pp in P.groupby(['game_id','period','deff'],sort=False):
    try: ff=Fg.get_group((g,p,d))
    except KeyError: continue
    limit=4 if p<=4 else 3
    fclk=ff.clk.to_numpy(); fidx=ff.fi.to_numpy(); fdr=ff.drawer.to_numpy()
    for st,lu in zip(pp.start_clk.to_numpy(),pp.off_lu):
        before=fclk>st
        k=before.sum()
        if k>=limit: sel=before
        elif st<=120:
            sel=before&(fclk<=120)
            if sel.sum()==0: continue
        else: continue
        n=sel.sum(); late=int(st<=120 or p>=5)
        lus=set(int(x) for x in lu)
        for i,dr in zip(fidx[sel],fdr[sel]):
            cr[i,2*late+(0 if int(dr) in lus else 1)]+=1.0/n
out=F[['game_id','period','clk','fouler_team','drawer']].copy()
out[['bc_main_on','bc_main_off','bc_late_on','bc_late_off']]=cr
out.to_parquet(f'{D}/cf/bc_{y}.parquet')
pen=sum(1 for _ in [])
print(y,'credit total',round(cr.sum()),'late share',round(cr[:,2:].sum()/max(cr.sum(),1),3),'off-floor share',round((cr[:,1]+cr[:,3]).sum()/max(cr.sum(),1),3),flush=True)
