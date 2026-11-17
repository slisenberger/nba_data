"""Offensive fouls drawn by defenders (charges and other offensive fouls), above expected: the defensive mirror of REAVES.
2005-06 onward (the play-by-play names the defender who drew it from then on; 2025-26 from the live feed).
Unit: clean-lineup defensive possessions. Expected offensive fouls per possession: LPM with FEs season, period x 3-min clock
bin, margin bin, start type, and the OFFENSE's team-season (an opponent that commits many is context). The defending team is
NOT in the expectation (that would absorb the defenders' skill).
Also in the expectation (audit): the offensive lineup's own offensive-foul rate (deciles).
Shares among the five defenders: 1/5 (raw) or a conditional logit on height, weight and role (usage, assists, 3PA and rim
share of their own shots), fitted on who drew each foul (role-adjusted = headline).
Value of a drawn offensive foul = that season's league points per possession (the opponent's possession is lost).
Outputs results/draw_def_seasons.csv, draw_def_career.csv."""
import pandas as pd, numpy as np, glob, os, subprocess, gc
from fe import fe_resid
from scipy.optimize import minimize
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
def clk_s(s):
    m,ss=s.split(':'); return int(m)*60+float(ss)
OF=[]
for y in range(2005,2025):
    tmp=f'{D}/tmp_of_{y}'; os.makedirs(tmp,exist_ok=True)
    subprocess.run(['tar','-xJf',f'/home/claude/shufinskiy/nba_data/datasets/nbastats_{y}.tar.xz','-C',tmp],check=True)
    f=[a for a in os.listdir(tmp) if a.endswith('.csv')][0]
    d=pd.read_csv(f'{tmp}/{f}',usecols=['GAME_ID','PERIOD','PCTIMESTRING','EVENTMSGTYPE','EVENTMSGACTIONTYPE','PLAYER1_ID','PLAYER1_TEAM_ID','PLAYER2_ID','PERSON2TYPE'],low_memory=False)
    subprocess.run(['rm','-rf',tmp])
    o=d[(d.EVENTMSGTYPE==6)&d.EVENTMSGACTIONTYPE.isin([4,26])&(d.PLAYER2_ID>0)&d.PERSON2TYPE.isin([4,5])].copy()
    OF.append(pd.DataFrame({'game_id':o.GAME_ID.astype('int64'),'period':o.PERIOD.astype(int),'clk':o.PCTIMESTRING.map(clk_s),'fouler':o.PLAYER1_ID.astype('int64'),
                            'off':o.PLAYER1_TEAM_ID.astype('int64'),'drawer':o.PLAYER2_ID.astype('int64'),'charge':(o.EVENTMSGACTIONTYPE==26).astype(int),'season':y}))
    print(y,len(o),flush=True)
c=pd.read_csv(f'{D}/cdn25/cdnnba_2025.csv',usecols=['gameId','period','clock','actionType','subType','descriptor','personId','teamId','foulDrawnPersonId'],low_memory=False)
o=c[(c.actionType=='foul')&(c.subType=='offensive')&c.foulDrawnPersonId.notna()&c.teamId.notna()]
cl=o.clock.str.extract(r'PT(\d+)M([\d.]+)S').astype(float)
OF.append(pd.DataFrame({'game_id':o.gameId.astype('int64'),'period':o.period.astype(int),'clk':(cl[0]*60+cl[1]).to_numpy(),'fouler':o.personId.astype('int64'),'off':o.teamId.astype('int64'),
                        'drawer':o.foulDrawnPersonId.astype('int64'),'charge':(o.descriptor=='charge').astype(int),'season':2025}))
OF=pd.concat(OF,ignore_index=True); print('offensive fouls',len(OF),' charges',int(OF.charge.sum()),flush=True)
# match each foul to its possession (same game, period, offense; clock inside the possession)
P=[]
for y in range(2005,2026):
    p=pd.read_parquet(f'{D}/out/poss_{y}.parquet',columns=['game_id','period','off','deff','start_clk','end_clk','margin','start_type','def_lu','off_lu','lq','pts'])
    p['season']=y; P.append(p)
P=pd.concat(P,ignore_index=True); P['pid_']=np.arange(len(P))
m=OF.merge(P[['game_id','period','off','start_clk','end_clk','pid_']],on=['game_id','period','off'])
m=m[(m.clk<=m.start_clk+0.05)&(m.clk>=m.end_clk-0.05)].sort_values('end_clk').drop_duplicates(['game_id','period','clk','fouler'],keep='first')
print('matched to a possession',round(len(m)/len(OF),3),flush=True)
P['n_of']=0; np.add.at(P['n_of'].values, m.pid_.to_numpy(), 1)
Q=P[(P.lq==1)&(P.def_lu.map(len)==5)].copy()
Q['clb']=np.where(Q.period<=4,Q.period.astype(int)*10+(Q.start_clk//180).astype(int),50+(Q.start_clk<=60).astype(int))
Q['mb']=(Q.margin.clip(-24,24)//6).astype(int); Q['ots']=Q.off.astype(np.int64)%100000*10000+Q.season
# audit: the offense's own players. Each offensive player's season rate of offensive fouls committed per on-court possession
# (shrunk with 300 pseudo-possessions); the lineup mean, in season deciles, enters the expectation (who you are defending is context)
ofr=OF.groupby(['fouler','season']).size().rename('c')
op=pd.DataFrame({'pid':np.concatenate(Q.off_lu.to_numpy()),'season':np.repeat(Q.season.to_numpy(),Q.off_lu.map(len).to_numpy())}).groupby(['pid','season']).size().rename('n')
pr=pd.concat([op,ofr.rename_axis(['pid','season'])],axis=1).fillna(0); k0=pr.c.sum()/pr.n.sum(); pr['r']=(pr.c+300*k0)/(pr.n+300)
rmap=pr.r.to_dict()
Q['olp']=[np.mean([rmap.get((p_,s_),k0) for p_ in l]) if len(l) else k0 for l,s_ in zip(Q.off_lu,Q.season)]
Q['f_olp']=Q.groupby('season').olp.transform(lambda v: pd.qcut(v.rank(method='first'),10,labels=False)).astype(int)
Q['f_s']=Q.season; Q['f_c']=Q.clb; Q['f_m']=Q.mb; Q['f_st']=pd.factorize(Q.start_type.astype(str))[0]
Q['y']=Q.n_of.astype(float); Q['xof']=Q.y-fe_resid(Q,'y',['f_s','f_c','f_m','f_st','ots','f_olp'],tol=1e-10)
ppp=P.groupby('season').pts.mean(); Q['val']=Q.season.map(ppp)
print('league offensive fouls per possession',round(Q.y.mean(),4),flush=True)
# drawn fouls on clean possessions, with the drawer's slot in the lineup
mm=m.merge(Q[['pid_','def_lu','season']],on='pid_',how='inner',suffixes=('','_q'))
mm['slot']=[list(l).index(d) if d in list(l) else -1 for l,d in zip(mm.def_lu,mm.drawer)]; mm=mm[mm.slot>=0]
LU=np.stack(Q.def_lu.to_numpy()); SEA=Q.season.to_numpy()
# role features (as def_reaves.py)
H=pd.read_csv(f'{D}/ext/heights.csv')[['shooter','height_in','weight']].rename(columns={'shooter':'pid'})
R0=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','season','off_poss']].rename(columns={'shooter':'pid'})
XA=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['season','shooter','z','kind']); XA=XA[(XA.kind!='fouled_miss')&(XA.season>=2005)]
zs=XA.assign(t3=XA.z.isin(['C3','AB3']),rim=XA.z.isin(['RA','PAINT'])).groupby(['shooter','season']).agg(t3=('t3','mean'),rimsh=('rim','mean'),fga=('z','size')).reset_index().rename(columns={'shooter':'pid'}); del XA
HD=pd.concat([pd.read_parquet(f) for f in glob.glob(f'{D}/handling/handling_*.parquet')])
RF=R0.merge(zs,on=['pid','season'],how='outer').merge(H,on='pid',how='left').merge(HD,on=['pid','season'],how='left')
RF['usage']=RF.fga/RF.off_poss; RF['ast_rate']=RF.ast/RF.off_poss
RF.loc[RF.off_poss.fillna(0)<200,['usage','ast_rate','t3','rimsh']]=np.nan
rf=['height_in','weight','usage','ast_rate','t3','rimsh']
for k in rf: RF[k]=(RF[k]-RF[k].mean())/RF[k].std()
RF[rf]=RF[rf].fillna(0); RF['usage2']=RF.usage**2; rf2=rf+['usage2']
fmap={(a,b):v for a,b,v in zip(RF.pid,RF.season,RF[rf2].to_numpy(np.float32))}; z0=np.zeros(len(rf2),np.float32)
def feats(lu,sea): return np.array([[fmap.get((p,s),z0) for p in l] for l,s in zip(lu,sea)],np.float32)
Fe=feats(np.stack(mm.def_lu.to_numpy()),mm.season.to_numpy()); ce=mm.slot.to_numpy()
def nll(b):
    u=Fe@b; u=u-u.max(1,keepdims=True); return -(u[np.arange(len(ce)),ce]-np.log(np.exp(u).sum(1))).sum()
def grad(b):
    u=Fe@b; u=u-u.max(1,keepdims=True); pr=np.exp(u); pr/=pr.sum(1,keepdims=True); return -(Fe[np.arange(len(ce)),ce]-(pr[:,:,None]*Fe).sum(1)).sum(0)
b=minimize(nll,np.zeros(len(rf2)),jac=grad,method='BFGS').x
print('role model coefs',dict(zip(rf2,b.round(3))),' pseudo-R2',round(1-nll(b)/(len(ce)*np.log(5)),3),flush=True)
sh=np.empty((len(Q),5),np.float32)
for k in range(0,len(Q),300000):
    sl=slice(k,k+300000); u=feats(LU[sl],SEA[sl])@b; u-=u.max(1,keepdims=True); e=np.exp(u); sh[sl]=e/e.sum(1,keepdims=True)
xo=Q.xof.to_numpy()[:,None]; v=Q.val.to_numpy()[:,None]
E1=pd.DataFrame({'pid':LU.ravel(),'season':np.repeat(SEA,5),'def_poss':1,'x_raw':(xo/5*np.ones((1,5))).ravel(),'x_role':(sh*xo).ravel(),
                 'xv_raw':(xo*v/5*np.ones((1,5))).ravel(),'xv_role':(sh*xo*v).ravel()})
S=E1.groupby(['pid','season']).sum()
A=mm.assign(v=mm.season.map(ppp)).groupby(['drawer','season']).agg(drawn=('slot','size'),charges=('charge','sum'),vd=('v','sum')).rename_axis(['pid','season'])
S=S.join(A,how='left').fillna(0).reset_index()
S['dd_raw']=S.vd-S.xv_raw; S['dd_role']=S.vd-S.xv_role
S['dd_raw_per100']=100*S.dd_raw/S.def_poss; S['dd_role_per100']=100*S.dd_role/S.def_poss
names=pd.concat([pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','name']].rename(columns={'shooter':'pid'}),pd.concat([pd.read_parquet(f) for f in glob.glob(f'{D}/out/names_*.parquet')])]).drop_duplicates('pid').set_index('pid').name
S['name']=S.pid.map(names); S.round(4).to_csv(f'{OUT}/draw_def_seasons.csv',index=False)
C=S.groupby('pid').agg(name=('name','first'),first=('season','min'),last=('season','max'),def_poss=('def_poss','sum'),drawn=('drawn','sum'),charges=('charges','sum'),
                       x_raw=('x_raw','sum'),x_role=('x_role','sum'),dd_raw=('dd_raw','sum'),dd_role=('dd_role','sum')).reset_index()
C['ratio_role']=C.drawn/C.x_role; C['per100_raw']=100*C.dd_raw/C.def_poss; C['per100_role']=100*C.dd_role/C.def_poss
C=C[C.def_poss>=15000]; C.round(4).to_csv(f'{OUT}/draw_def_career.csv',index=False)
c=['name','first','last','def_poss','drawn','charges','x_role','ratio_role','dd_role','per100_role','per100_raw']
print('players',len(C)); print('TOP (role-adjusted per 100 def poss)'); print(C.sort_values('per100_role',ascending=False)[c].head(20).round(2).to_string(index=False))
print('TOP total drawn'); print(C.sort_values('drawn',ascending=False)[c].head(10).round(2).to_string(index=False))
print('BOTTOM'); print(C.sort_values('per100_role')[c].head(8).round(2).to_string(index=False))
chk=['Chris Paul','Manu Ginobili','Marcus Smart','Patrick Beverley','Shane Battier','Ersan Ilyasova','Kyle Lowry','Anderson Varejao','Nick Collison','Derek Fisher','James Harden','Joel Embiid']
print(C[C.name.isin(chk)][c].assign(rk=C.per100_role.rank(ascending=False)).round(2).sort_values('rk').to_string(index=False))
