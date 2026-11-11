"""Foul EPA (proposed work item 3): expected points added by each drawn counting foul, in its game state.
  EPA = immediate + bonus + whistle-slope + foul trouble
  immediate:  shooting foul -> points of the fouled attempt at league FT% minus the league value of an unfouled
              FGA from the same zone (fouled misses spread over zones as in xfoul.py);
              non-shooting foul -> E[possession pts | such a foul (bonus / not) at possession age e]
              minus E[possession pts | possession still alive at age e] (league, by 2-s age bin and bonus state).
  bonus:      opponent bonus possessions this foul creates (cf.py, single-foul removal) x beta_bonus.
  slope:      remaining opponent possessions in the period x gamma (per-team-foul effect below the bonus), only if the
              game-FE test supports it (GAMMA env / res_bonus_gfe.csv).
  trouble:    possessions the fouler loses (res_minutes: by period and nth foul) x delta, for top-5 foulers only.
  bonus is split into bonus_on / bonus_off: the created bonus possessions with the drawer on / off the floor.
  Late-game intentional fouls (defense trailing, last 2:00 of Q4/OT, non-shooting) and take fouls are flagged and
  excluded from drawer totals (the defense chose them). Foul trouble uses res_minutes_clock.csv (quarter x nth foul x
  3-min clock bin). Valued at league FT% so it measures drawing, not the drawer's FT shooting; 'act' columns use actual points."""
import pandas as pd, numpy as np, glob, json, os, gc
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
B=json.load(open(f'{D}/betas.json'))
BETA=float(os.environ.get('BETA',B['beta_mid'])); BETA_LATE=float(os.environ.get('BETA_LATE',B.get('beta_late',B['beta_mid']))); DELTA=float(os.environ.get('DELTA',B['delta_poss'])); GAMMA=float(os.environ.get('GAMMA',0))
print('beta',BETA,'beta_late',BETA_LATE,'delta',DELTA,'gamma',GAMMA)
names=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'{D}/out/names_*.parquet'))]).drop_duplicates('pid').set_index('pid').name
F=pd.concat([pd.read_parquet(f,columns=['game_id','period','clk','season','fouler','fouler_team','drawer_team','drawer','imputed','action','pen_before','fouler_pf','flips_single','flips_single_on','tf_after']) for f in sorted(glob.glob(f'{D}/cf/cf_fouls_*.parquet'))],ignore_index=True)
F['shoot']=F.action.isin([2,29]).astype(int)
# ---- possession context for each foul: possession age, remaining opp possessions in period
P=pd.concat([pd.read_parquet(f'{D}/out/poss_{y}.parquet',columns=['game_id','period','off','home_off','start_clk','end_clk','pts','margin']) for y in range(1996,2026)],ignore_index=True)
P['dur']=P.start_clk-P.end_clk
key=lambda g,p,t: (g.astype(np.int64)*10+p.astype(np.int64))*100+(t.astype(np.int64)%100)   # team ids 1610612737-66: %100 is unique (was %2: a bug, audit Nov 2026)
P['k']=key(P.game_id,P.period,P.off); F['k']=key(F.game_id,F.period,F.drawer_team)
P=P.sort_values(['k','start_clk'],ascending=[True,False]).reset_index(drop=True)
# possession containing the foul: last possession of the drawer team with start_clk >= clk
Fs=F[['k','clk']].copy(); Fs['neg']=-Fs.clk; Fs['i']=np.arange(len(F))
Pp=P[['k','start_clk']].copy(); Pp['neg']=-Pp.start_clk; Pp['pi']=np.arange(len(P))
Fs=Fs.sort_values('neg'); Pp=Pp.sort_values('neg')
m=pd.merge_asof(Fs,Pp[['neg','k','pi']],on='neg',by='k',direction='backward').sort_values('i')
pi=m.pi.to_numpy()
ok=~np.isnan(pi); pi=np.where(ok,pi,0).astype(int)
F['age']=np.where(ok,P.start_clk.to_numpy()[pi]-F.clk.to_numpy(),np.nan)
F['poss_pts']=np.where(ok,P.pts.to_numpy()[pi],np.nan)
F['margin']=np.where(ok,P.margin.to_numpy()[pi],np.nan)       # drawer team's lead at the start of the possession
F['home_drawer']=np.where(ok,P.home_off.to_numpy()[pi],np.nan)
# remaining drawer-team possessions in period that start after the foul (bonus/whistle exposure)
grp_end=P.groupby('k').cumcount(ascending=False).to_numpy()  # possessions after this one in same k
F['rem_opp_poss']=np.where(ok,grp_end[pi],np.nan)
# ---- EP of a live possession by age, and of a possession given a non-shooting foul at age e
P['ab']=0
ages=np.arange(0,24,2)
alive=np.array([P.loc[P.dur>a,'pts'].mean() for a in ages])
F['abin']=np.clip((F.age//2).fillna(0).astype(int),0,len(ages)-1)
F['ep_alive']=alive[F.abin]
ns=F[(F.shoot==0)&F.age.notna()]
F['eop']=(F.clk<24).astype(int)     # under 24 s left in the period: no full possession left
P['eop']=(P.start_clk<24).astype(int)
alive_e={(e,a):P.loc[(P.dur>ages[a])&(P.eop==e),'pts'].mean() for e in (0,1) for a in range(len(ages))}
F['ep_alive']=[alive_e.get((e,a),np.nan) for e,a in zip(F.eop,F.abin)]
ns=F[(F.shoot==0)&F.age.notna()]
cell=ns.groupby(['pen_before','abin','eop']).poss_pts.mean()
F['ep_after_ns']=[cell.get((p,a,e),np.nan) for p,a,e in zip(F.pen_before,F.abin,F.eop)]
# ---- shooting fouls: attempt-level immediate value
X=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['game_id','season','kind','z','w','is3','made','pts','att_id','fouler','period','clk','dv','fouled'])
vn=X[X.kind=='fga'].groupby(['season','z']).pts.mean().rename('VN')
X=X.join(vn,on=['season','z'])
A=pd.concat([pd.read_parquet(f,columns=['game_id','kind','fta','ftm']) for f in sorted(glob.glob(f'{D}/out/att_*.parquet'))])
sh=A[A.kind.isin(['and1','fouled_miss'])]
lgft=sh.ftm.sum()/sh.fta.sum()
S=X[X.fouled==1].copy()
S['nft']=np.where(S.kind=='and1',1,np.where(S.is3==1,3,2))
S['imm_exp']=S.dv*S.w     # league value of a fouled vs unfouled attempt from that zone: drawing only (an and-one's make is not credited)
S['imm_made']=(S.made*np.where(S.is3==1,3,2)+S.nft*lgft-S.VN)*S.w   # old version, credits the make
S['imm_act']=(S.pts-S.VN)*S.w
SI=S.groupby('att_id').agg(game_id=('game_id','first'),period=('period','first'),clk=('clk','first'),fouler=('fouler','first'),imm_exp=('imm_exp','sum'),imm_made=('imm_made','sum'),imm_act=('imm_act','sum'))
SI=SI.drop_duplicates(['game_id','period','clk','fouler'])
F=F.merge(SI,on=['game_id','period','clk','fouler'],how='left')
print('shooting fouls matched to an attempt',F.loc[F.shoot==1,'imm_exp'].notna().mean().round(4),'league shoot FT%',round(lgft,4))
F['imm']=np.where(F.shoot==1,F.imm_exp,F.ep_after_ns-F.ep_alive)
F['imm']=F.imm.fillna(F.groupby(['shoot','pen_before']).imm.transform('mean'))
# ---- foul trouble
M=pd.read_csv(f'{OUT}/res_minutes.csv') if os.path.exists(f'{OUT}/res_minutes.csv') else pd.read_csv('/home/user/nba_data/ft_study/results/handoff/res_minutes.csv')
ml={(r.period,r.nth_foul):r.marginal_poss_lost for r in M.itertuples()}
def lost(p,n):
    if n<=1 or p>4: return 0.0
    if (p,n) in ml: return ml[(p,n)]
    ks=[k for k in ml if k[0]==p and k[1]<=n]; return ml[max(ks)] if ks else 0.0
MC=pd.read_csv(f'{OUT}/res_minutes_clock.csv')
mlc={(r.period,r.nth_foul,r.clock_bin):r.marginal_poss_lost for r in MC.itertuples() if r.marginal_poss_lost==r.marginal_poss_lost}
F['cb']=np.minimum(F.clk//180,3).astype(int)
F['poss_lost_flat']=[lost(p,n) if f>0 else 0.0 for p,n,f in zip(F.period,F.fouler_pf,F.fouler)]
F['poss_lost']=[max(mlc.get((p,n,b),fl),0.0) if f>0 and p<=4 else 0.0 for p,n,b,f,fl in zip(F.period,F.fouler_pf,F.cb,F.fouler,F.poss_lost_flat)]
PT=pd.read_parquet(f'{D}/player_team.parquet')
PT['rk']=PT.groupby(['team','season']).poss_both.rank(ascending=False,method='first')
top5=set(zip(PT[PT.rk<=5].pid,PT[PT.rk<=5].season))
F['fouler_top5']=[int((f,s) in top5) for f,s in zip(F.fouler,F.season)]
F['trouble']=F.poss_lost*DELTA*F.fouler_top5
BC=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'{D}/cf/bc_*.parquet'))],ignore_index=True)
assert len(BC)==len(F), (len(BC),len(F))   # same row order as cf_fouls
for c in ['bc_main_on','bc_main_off','bc_late_on','bc_late_off']: F[c]=BC[c].to_numpy()
F['bonus_pivotal_old']=F.flips_single*BETA      # old single-removal credit (over-counts ~1.5x)
F['bonus']=BETA*(F.bc_main_on+F.bc_main_off)+BETA_LATE*(F.bc_late_on+F.bc_late_off)
F['bonus_on']=BETA*F.bc_main_on+BETA_LATE*F.bc_late_on      # its share of bonus possessions with the drawer on the floor
F['bonus_off']=F.bonus-F.bonus_on                              # ... and with him on the bench
F['slope']=F.rem_opp_poss.fillna(0)*GAMMA*(F.period<=4)
F['epa']=F.imm+F.bonus+F.slope+F.trouble
F['epa_act']=np.where(F.shoot==1,F.imm_act.fillna(F.imm),F.imm)+F.bonus+F.slope+F.trouble
# ---- flags: late intentional fouls (defense trailing, last 2:00 of Q4/OT, non-shooting) and take fouls are the
# defense's choice, not drawn; garbage time = lead >= 20 in Q4/OT
F['intentional']=((F.shoot==0)&(F.period>=4)&(F.clk<=120)&(F.margin>0)).astype(int)
F['take_foul']=F.action.isin([28,31]).astype(int)
F['garbage']=((F.period>=4)&(F.margin.abs()>=20)).astype(int)
# hack-a fouls: non-shooting fouls in the bonus on a poor FT shooter (< 55% that season, min 50 FTA) away from the
# last 2:00 -- the defense's choice, like late intentional fouls (audit)
_ft=pd.concat([pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['shooter','fta','ftm']).assign(season=y) for y in range(1996,2026)])
_ft=_ft.groupby(['shooter','season'])[['fta','ftm']].sum(); _ft=_ft[_ft.fta>=50]; poor=set(_ft[_ft.ftm/_ft.fta<0.55].index)
F['hack']=((F.shoot==0)&(F.pen_before==1)&(F.intentional==0)&np.array([(d,s_) in poor for d,s_ in zip(F.drawer,F.season)])).astype(int)
F['skill']=((F.intentional==0)&(F.take_foul==0)&(F.hack==0)).astype(int)
F.drop(columns=['k']).to_parquet(f'{D}/foul_epa.parquet')
print('flags: hack',round(F.hack.mean(),4),'intentional',round(F.intentional.mean(),4),'take',round(F.take_foul.mean(),4),'garbage',round(F.garbage.mean(),4))
print('mean poss lost per foul: flat',round(F.poss_lost_flat.mean(),3),' clock-dependent',round(F.poss_lost.mean(),3))
pd.set_option('display.width',250)
F['type']=np.where(F.shoot==1,'shooting',np.where(F.pen_before==1,'bonus_nonshoot','nonbonus_nonshoot'))
print(F.groupby('type')[['imm','bonus','bonus_on','bonus_off','trouble','epa']].mean().round(4).to_string())
print(F.groupby(['period'])[['bonus','trouble','epa']].mean().round(4).to_string())
F.groupby(['season','type'])[['imm','bonus','slope','trouble','epa']].mean().round(4).to_csv(f'{OUT}/foul_epa_by_type.csv')
# ---- drawer careers
F=F[(F.drawer>0)&(F.skill==1)]   # drawn fouls only: no late intentional or take fouls
on=pd.concat([pd.read_parquet(f,columns=['pid','season','off_poss']) for f in sorted(glob.glob(f'{D}/cf/cf_players_*.parquet'))])
gp=pd.concat([pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','shooter']).assign(season=y) for y in range(1996,2026)])
gp=gp[gp.shooter>0].groupby('shooter').game_id.nunique().rename('gp')
C=F.groupby('drawer').agg(bonus_on=('bonus_on','sum'),bonus_off=('bonus_off','sum'),fouls=('epa','size'),shoot=('shoot','sum'),epa=('epa','sum'),epa_act=('epa_act','sum'),imm=('imm','sum'),bonus=('bonus','sum'),slope=('slope','sum'),trouble=('trouble','sum'),imputed=('imputed','mean'),first=('season','min'),last=('season','max'))
C=C.join(on.groupby('pid').off_poss.sum()).join(gp)
C['bonus_off_per_g']=C.bonus_off/C.gp; C['epa_per100']=100*C.epa/C.off_poss; C['epa_per_g']=C.epa/C.gp; C['epa_per_foul']=C.epa/C.fouls
C['name']=C.index.map(names)
C=C[C.off_poss>=20000]
C.reset_index().rename(columns={'drawer':'pid'}).round(4).to_csv(f'{OUT}/foul_epa_career.csv',index=False)
cols=['name','first','last','gp','fouls','imm','bonus_on','bonus_off','trouble','epa','epa_per_g','epa_per100','epa_per_foul','bonus_off_per_g','imputed']
print(C.sort_values('bonus_off',ascending=False)[cols].head(15).round(3).to_string())
print(C.sort_values('epa',ascending=False)[cols].head(30).round(3).to_string())
print(C.sort_values('epa_per100',ascending=False)[cols].head(20).round(3).to_string())
