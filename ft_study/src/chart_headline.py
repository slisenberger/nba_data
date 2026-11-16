"""Bar chart of a player's headline REAVES (size-adjusted shooting + role-adjusted non-shooting, per 100 possessions) by
season. Seasons before 2005-06 (no non-shooting data) show shooting only, in a lighter shade.
Usage: python3 chart_headline.py "Player Name" [out.png]"""
import sys, pandas as pd, numpy as np, matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
R='/home/user/nba_data/ft_study/results'
name=sys.argv[1]; out=sys.argv[2] if len(sys.argv)>2 else f"{R}/charts/{name.lower().replace(' ','_').replace('.','')}_headline_reaves.png"
S=pd.read_csv(f'{R}/reaves_seasons.csv'); N=pd.read_csv(f'{R}/ns_reaves_seasons.csv')
m=S[S.name==name][['season','reaves_size_per100poss','off_poss','reaves_size']].merge(N[N.name==name][['season','ns_reaves_role','poss','ns_reaves_role_per100poss']],on='season',how='left').sort_values('season').reset_index(drop=True)
m['ns']=m.ns_reaves_role_per100poss; m['hl']=m.reaves_size_per100poss+m.ns.fillna(0); m['partial']=m.ns.isna()
ok=m.ns.notna(); car=100*m.reaves_size.sum()/m.off_poss.sum()+(100*m.ns_reaves_role[ok].sum()/m.poss[ok].sum() if ok.any() else 0)
lab=[f"{y}-{str(y+1)[2:]}" for y in m.season]
INK,INK2,GRID,BG,BLUE,LIGHT='#0b0b0b','#52514e','#e1e0d9','#fcfcfb','#2a78d6','#9ec5f0'
fig,ax=plt.subplots(figsize=(max(6.5,0.45*len(m)+3),4.4),dpi=200); fig.patch.set_facecolor(BG); ax.set_facecolor(BG)
ax.bar(range(len(m)),m.hl,width=0.62,color=[LIGHT if p else BLUE for p in m.partial],zorder=3)
ax.axhline(0,color=INK2,lw=1,zorder=4)
ax.axhline(car,color=INK2,lw=1,ls=(0,(4,3)),zorder=2)
ax.text(len(m)-0.55,car+(0.03 if car>=0 else -0.03),f'career {car:+.2f}'.replace('-','−'),ha='right',va='bottom' if car>=0 else 'top',fontsize=8.5,color=INK2)
for i in {m.hl.idxmax(),m.hl.idxmin()}:
    v=m.hl[i]; ax.text(i,v+(0.025 if v>0 else -0.025),f"{v:+.2f}".replace('-','−'),ha='center',va='bottom' if v>0 else 'top',fontsize=9,color=INK)
ax.set_xticks(range(len(m))); ax.set_xticklabels(lab,rotation=45,ha='right',fontsize=8.5,color=INK2)
ax.tick_params(axis='y',labelsize=8.5,colors=INK2,length=0); ax.tick_params(axis='x',length=0)
ax.yaxis.grid(True,color=GRID,lw=0.8,zorder=0); [s.set_visible(False) for s in ax.spines.values()]
lo,hi=min(m.hl.min(),car,0),max(m.hl.max(),car,0); pad=0.18*(hi-lo); ax.set_ylim(lo-pad,hi+pad)
fig.text(0.01,0.97,f"{name}: headline REAVES by season",fontsize=12.5,weight='bold',color=INK,va='top')
fig.text(0.01,0.905,"Points per 100 possessions from drawing fouls above expected, size- and role-adjusted. Above 0 = draws more than expected.",fontsize=8.5,color=INK2,va='top')
note="Source: NBA play-by-play, regular seasons."+(" Light bars: shooting fouls only (no non-shooting data before 2005-06)." if m.partial.any() else "")
fig.text(0.01,0.015,note,fontsize=7.5,color=INK2)
fig.subplots_adjust(left=0.07,right=0.99,top=0.83,bottom=0.2)
fig.savefig(out,facecolor=BG); print(out); print(m[['season','hl']].round(2).to_string(index=False)); print('career',round(car,2))
