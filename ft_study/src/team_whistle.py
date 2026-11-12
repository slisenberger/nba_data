"""Do teams get systematically different whistles? Uses xFoul residuals (fouled - xF) on the offense's shooting
attempts. xF has no home/away or offense-team term, so both show up in residuals. Cells = player x game; team identity
from att_*.parquet (the shooter's offense that game), home from poss_*.parquet.
 1) Home edge: home - away residual per 100 attempts / per 100 team possessions, by season (2020-21: mostly empty arenas).
 2) Team effects net of players: residual per attempt = player FE + offense team-season FE (+ home), alternating
    weighted least squares on player-game cells. Persistence of franchise effects across seasons; franchise means vs noise.
Valued in points with the attempt's dv (league value of a fouled vs unfouled attempt)."""
import pandas as pd, numpy as np, glob
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
X=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['game_id','season','shooter','w','fouled','xf','dv'])
X['r']=(X.fouled-X.xf)*X.w; X['rp']=X.r*X.dv
G=X.groupby(['game_id','shooter']).agg(season=('season','first'),att=('w','sum'),r=('r','sum'),rp=('rp','sum')).reset_index()
tm=[];hm=[];tp=[]
for y in range(1996,2026):
    a=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','shooter','off','kind']); a=a[a.kind.isin(['fga','and1','fouled_miss'])&(a.shooter>0)]
    tm.append(a.groupby(['game_id','shooter']).off.agg(lambda s:s.mode().iloc[0]).reset_index())
    p=pd.read_parquet(f'{D}/out/poss_{y}.parquet',columns=['game_id','off','home_off'])
    hm.append(p[p.home_off==1].groupby('game_id').off.first().rename('home').reset_index())
    tp.append(p.groupby(['game_id','off']).size().rename('poss').reset_index())
G=G.merge(pd.concat(tm),on=['game_id','shooter']).merge(pd.concat(hm),on='game_id')
G['is_home']=(G.off==G.home).astype(int)
TP=pd.concat(tp)
# ---- 1) home edge by season (raw residuals; player mix is the same team at home and away)
T=G.groupby(['season','game_id','off','is_home']).agg(att=('att','sum'),r=('r','sum'),rp=('rp','sum')).reset_index().merge(TP,on=['game_id','off'])
H=T.groupby(['season','is_home'])[['att','r','rp','poss']].sum().unstack()
he=pd.DataFrame({'home_r100att':100*H['r'][1]/H['att'][1],'away_r100att':100*H['r'][0]/H['att'][0],
                 'home_pts100poss':100*H['rp'][1]/H['poss'][1],'away_pts100poss':100*H['rp'][0]/H['poss'][0]})
he['edge_fouls_per100att']=he.home_r100att-he.away_r100att; he['edge_pts_per100poss']=he.home_pts100poss-he.away_pts100poss
# game-level SE of the edge per season (games as clusters)
gg=T.assign(sgn=np.where(T.is_home==1,1,-1)); 
def se(s):
    g=s.groupby('game_id').apply(lambda d: pd.Series({'d':(d.rp*d.sgn).sum(),'p':d.poss.sum()/2}))
    return 100*g.d.std()*np.sqrt(len(g))/g.p.sum()
he['se_pts_per100poss']=[se(gg[gg.season==s]) for s in he.index]
he.round(4).to_csv(f'{OUT}/whistle_home_edge.csv'); print(he.round(3).to_string())
# ---- 2) player FE + offense team-season FE + home, alternating weighted LS
G['ts']=G.off.astype(np.int64)*10000+G.season
y=(G.r/G.att).to_numpy(); w=G.att.to_numpy(); yp=(G.rp/G.att).to_numpy()
def als(yv):
    pl=pd.factorize(G.shooter)[0]; ts=pd.factorize(G.ts)[0]; h=G.is_home.to_numpy()
    a=np.zeros(pl.max()+1); b=np.zeros(ts.max()+1); c=0.0
    for it in range(200):
        a_new=np.bincount(pl,w*(yv-b[ts]-c*h))/np.bincount(pl,w)
        b_new=np.bincount(ts,w*(yv-a_new[pl]-c*h))/np.bincount(ts,w)
        c_new=np.sum(w*h*(yv-a_new[pl]-b_new[ts]))/np.sum(w*h*h) if False else np.sum(w*(h-np.average(h,weights=w))*(yv-a_new[pl]-b_new[ts]))/np.sum(w*(h-np.average(h,weights=w))**2)
        d=max(np.abs(a_new-a).max(),np.abs(b_new-b).max(),abs(c_new-c)); a,b,c=a_new,b_new,c_new
        if d<1e-9: break
    return a,b,c,pl,ts
a,b,c,pl,ts=als(yp)
tsk=pd.factorize(G.ts)[1]
B=pd.DataFrame({'ts':tsk,'eff':b}); B['team']=B.ts//10000; B['season']=B.ts%10000
B['eff']=B.eff-B.groupby('season').eff.transform('mean')                     # relative to the season's average team
posw=G.groupby('ts').att.sum(); B['att']=B.ts.map(posw)
tpp=TP.merge(G[['game_id','season']].drop_duplicates(),on='game_id').assign(ts=lambda d:d.off.astype(np.int64)*10000+d.season).groupby('ts').poss.sum()
B['att_per_poss']=B.att/B.ts.map(tpp)
B['pts_per100poss']=100*B.eff*B.att_per_poss                                # team whistle effect in points per 100 team possessions
# sampling noise of a team-season effect (binomial): sd of residual per attempt ~ sqrt(mean xf(1-xf))
noise_att=np.sqrt(np.average(X.xf*(1-X.xf),weights=X.w))*X.dv.mean()
B['se_pts_per100poss']=100*noise_att/np.sqrt(B.att)*B.att_per_poss
print('home effect net of players and team-seasons (pts per attempt):',round(c,5),' -> per 100 poss ~',round(100*c*B.att_per_poss.mean(),3))
B=B.sort_values(['team','season'])
B['next']=B.groupby('team').pts_per100poss.shift(-1)
pers=B.dropna(subset=['next']); r1=np.corrcoef(pers.pts_per100poss,pers.next)[0,1]
print('year-to-year correlation of team whistle effects (net of players):',round(r1,3),' n',len(pers))
sd_true=np.sqrt(max(B.pts_per100poss.var()-np.mean(B.se_pts_per100poss**2),0))
print('team-season effect SD (pts/100 poss): observed',round(B.pts_per100poss.std(),3),' noise',round(np.sqrt(np.mean(B.se_pts_per100poss**2)),3),' signal',round(sd_true,3))
names={1610612747:'Lakers',1610612738:'Celtics',1610612744:'Warriors',1610612748:'Heat',1610612759:'Spurs',1610612752:'Knicks',1610612745:'Rockets',1610612746:'Clippers',1610612741:'Bulls',1610612765:'Pistons',1610612755:'76ers',1610612760:'Thunder/Sonics',1610612742:'Mavericks',1610612749:'Bucks',1610612743:'Nuggets',1610612756:'Suns',1610612757:'Blazers',1610612758:'Kings',1610612762:'Jazz',1610612750:'Wolves',1610612737:'Hawks',1610612739:'Cavaliers',1610612754:'Pacers',1610612753:'Magic',1610612751:'Nets',1610612761:'Raptors',1610612764:'Wizards',1610612763:'Grizzlies',1610612766:'Hornets',1610612740:'Pelicans/Hornets'}
F=B.groupby('team').apply(lambda d: pd.Series({'seasons':len(d),'mean_pts100':d.pts_per100poss.mean(),'se':np.sqrt((d.se_pts_per100poss**2).sum())/len(d),'share_positive':(d.pts_per100poss>0).mean()})).reset_index()
F['name']=F.team.map(names); F['z']=F.mean_pts100/F.se
chi=(F.z**2).sum(); print('franchise means: sum z^2 =',round(chi,1),'on',len(F),'franchises (pure noise ~',len(F),')')
F.sort_values('mean_pts100',ascending=False).round(3).to_csv(f'{OUT}/whistle_franchise.csv',index=False)
B.round(4).to_csv(f'{OUT}/whistle_team_seasons.csv',index=False)
print(F.sort_values('mean_pts100',ascending=False)[['name','seasons','mean_pts100','se','z','share_positive']].round(2).to_string(index=False))
L=B[B.team==1610612747][['season','pts_per100poss','se_pts_per100poss']]; print('Lakers by season'); print(L.round(2).to_string(index=False))
# ---- 3) split each team-season effect into home and away games (same player FE model, team x season x home FE)
G['tsh']=G.ts*10+G.is_home
pl=pd.factorize(G.shooter)[0]; th=pd.factorize(G.tsh)[0]; a2=np.zeros(pl.max()+1); b2=np.zeros(th.max()+1)
for it in range(300):
    a_new=np.bincount(pl,w*(yp-b2[th]))/np.bincount(pl,w); b_new=np.bincount(th,w*(yp-a_new[pl]))/np.bincount(th,w)
    d=max(np.abs(a_new-a2).max(),np.abs(b_new-b2).max()); a2,b2=a_new,b_new
    if d<1e-9: break
K=pd.DataFrame({'tsh':pd.factorize(G.tsh)[1],'eff':b2}); K['home']=K.tsh%10; K['ts']=K.tsh//10; K['team']=K.ts//10000; K['season']=K.ts%10000
K['eff']=K.eff-K.groupby(['season','home']).eff.transform('mean')
K['pts100']=100*K.eff*K.ts.map(B.set_index('ts').att_per_poss)
HA=K.groupby(['team','home']).pts100.mean().unstack(); HA.columns=['away','home']; HA['name']=HA.index.map(names)
print('franchise effect (pts/100 poss) in away vs home games; corr across franchises',round(HA[['away','home']].corr().iloc[0,1],3))
print(HA.sort_values('away').round(2).to_string())
HA.round(3).to_csv(f'{OUT}/whistle_franchise_home_away.csv')
# (Nov 12 follow-ups, run interactively: regressing team-season effects on team shot style -- drive / pull-up / post /
#  dunk-tip / transition / rim / 3PA shares -- gave R^2 0.11; on team net rating, corr -0.07. Franchise ranking and the
#  0.45 year-to-year persistence survive both.)
