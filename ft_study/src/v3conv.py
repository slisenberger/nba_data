"""Convert nbastatsv3 play-by-play (2025-26) to the nbastats v2 columns parse.py reads.
v3 differences handled: one row per sub with the outgoing player's id (incoming player named by last name only ->
resolved against the team's players in that game, preferring one not on the floor); steals / blocks are separate
rows sharing the turnover's / shot's actionNumber (folded into PLAYER2 / PLAYER3); no fouled player in v3 (taken from
the live feed when its CSV is given as a 3rd argument; else PLAYER2 of a foul is left 0 and cf.py imputes); clock 'PT11M36.00S' -> '11:36'."""
import pandas as pd, numpy as np, sys, re, unicodedata
norm=lambda x: unicodedata.normalize('NFKD',str(x)).encode('ascii','ignore').decode().strip()
src,dst=sys.argv[1],sys.argv[2]
# optional 3rd arg: the NBA live feed (cdnnba) CSV for the same season -> fouled player on fouls (PLAYER2)
DRAWN={}
if len(sys.argv)>3:
    _c=pd.read_csv(sys.argv[3],low_memory=False,usecols=['gameId','actionNumber','actionType','foulDrawnPersonId'])
    _c=_c[(_c.actionType=='foul')&_c.foulDrawnPersonId.notna()]
    DRAWN={(int(g),int(a)):int(x) for g,a,x in zip(_c.gameId,_c.actionNumber,_c.foulDrawnPersonId)}
d=pd.read_csv(src,low_memory=False)
d['personId']=d.personId.fillna(0).astype('int64'); d['teamId']=d.teamId.fillna(0).astype('int64')
ET={'Made Shot':1,'Missed Shot':2,'Free Throw':3,'Rebound':4,'Turnover':5,'Foul':6,'Violation':7,'Substitution':8,'Timeout':9,'Jump Ball':10,'Ejection':11,'Instant Replay':18}
FOUL={'Personal':1,'Shooting':2,'Loose Ball':3,'Offensive':4,'Inbound':5,'Away From Play':6,'Punching':8,'Clear Path':9,'Double Personal':10,'Technical':11,'Non-Unsportsmanlike Technical':12,'Hanging Technical':13,'Flagrant Type 1':14,'Flagrant Type 2':15,'Double Technical':16,'Defense 3 Second':17,'Delay Technical':18,'Taunting':19,'Excess Timeout Technical':25,'Offensive Charge':26,'Personal Block':27,'Personal Take':28,'Shooting Block':29,'Flopping':30,'Transition Take':31}
FT={'Free Throw 1 of 1':10,'Free Throw 1 of 2':11,'Free Throw 2 of 2':12,'Free Throw 1 of 3':13,'Free Throw 2 of 3':14,'Free Throw 3 of 3':15,'Free Throw Technical':16,'Free Throw Flagrant 1 of 2':18,'Free Throw Flagrant 2 of 2':19,'Free Throw Flagrant 1 of 1':20,'Free Throw Technical 1 of 2':21,'Free Throw Technical 2 of 2':22,'Free Throw Clear Path 1 of 2':25,'Free Throw Clear Path 2 of 2':26,'Free Throw Flagrant 1 of 3':27,'Free Throw Flagrant 2 of 3':28,'Free Throw Flagrant 3 of 3':29}
out=[]
def clk(s):
    m=re.match(r'PT(\d+)M([\d.]+)S',str(s)); return f"{int(m.group(1))}:{int(float(m.group(2))):02d}" if m else "0:00"
unres=0
for gid,g in d.groupby('gameId',sort=False):
    g=g.sort_values(['actionNumber','actionId'])
    pl=g[(g.teamId>0)&(g.personId>0)]
    teams=sorted(pl.teamId.unique())
    if len(teams)!=2: continue
    loc={t:pl[pl.teamId==t].location.mode().iloc[0] for t in teams}
    roster={t:[(pid,{norm(a),norm(b)}) for pid,a,b in pl[pl.teamId==t].drop_duplicates('personId')[['personId','playerName','playerNameI']].values.tolist()] for t in teams}
    steal={}; blk={}
    for r in g[g.actionType.isna()&g.description.notna()].itertuples():
        if 'STEAL' in r.description: steal[r.actionNumber]=(r.personId,r.location)
        elif 'BLOCK' in r.description: blk[r.actionNumber]=(r.personId,r.location)
    on={t:set() for t in teams}
    for r in g.itertuples():
        at=r.actionType
        if not isinstance(at,str): continue
        if at=='period':
            et=12 if r.subType=='start' else 13; act=0
        elif at=='Heave': continue
        else:
            et=ET.get(at)
            if et is None: continue
            act=FOUL.get(r.subType,0) if et==6 else FT.get(r.subType,0) if et==3 else 0
        team=r.teamId if r.teamId>0 else 0
        p1=r.personId; p1team=team if team else np.nan
        isplayer=team>0 and p1>0
        ptype=(4 if r.location=='h' else 5) if isplayer else ((2 if r.location=='h' else 3) if p1>0 else 0)
        p2=0;p2t=np.nan;pt2=0;p3=0;p3t=np.nan;pt3=0
        desc=r.description if isinstance(r.description,str) else ''
        if et==8:
            m=re.match(r'SUB: (.+) FOR (.+)$',desc)
            if m and team:
                nm=norm(m.group(1))
                c=[pid for pid,n in roster[team] if nm in n and pid!=p1]
                if len(c)>1: c=[x for x in c if x not in on[team]] or c
                if c: p2=int(c[0]); p2t=team; pt2=ptype
                else: unres+=1
                on[team].discard(p1); 
                if p2: on[team].add(p2)
        elif isplayer and team in on: on[team].add(p1)
        if et==6 and team and (int(gid),int(r.actionNumber)) in DRAWN:
            p2=DRAWN[(int(gid),int(r.actionNumber))]; pt2=5 if r.location=='h' else 4; p2t=[t for t in teams if t!=team][0]
        if et==5 and r.actionNumber in steal:
            s=steal[r.actionNumber]; p2=int(s[0]); pt2=4 if s[1]=='h' else 5; p2t=[t for t in teams if t!=team][0] if team else np.nan
        if et==2 and r.actionNumber in blk:
            s=blk[r.actionNumber]; p3=int(s[0]); pt3=4 if s[1]=='h' else 5; p3t=[t for t in teams if t!=team][0] if team else np.nan
        h=desc if r.location=='h' else np.nan; v=desc if r.location=='v' else np.nan
        out.append((int(gid),int(r.actionNumber),et,act,int(r.period),clk(r.clock),h,v,ptype,p1,r.playerName if isplayer else np.nan,p1team,pt2,p2,np.nan,p2t,pt3,p3,np.nan,p3t))
cols=['GAME_ID','EVENTNUM','EVENTMSGTYPE','EVENTMSGACTIONTYPE','PERIOD','PCTIMESTRING','HOMEDESCRIPTION','VISITORDESCRIPTION','PERSON1TYPE','PLAYER1_ID','PLAYER1_NAME','PLAYER1_TEAM_ID','PERSON2TYPE','PLAYER2_ID','PLAYER2_NAME','PLAYER2_TEAM_ID','PERSON3TYPE','PLAYER3_ID','PLAYER3_NAME','PLAYER3_TEAM_ID']
O=pd.DataFrame(out,columns=cols)
assert not O.duplicated(['GAME_ID','EVENTNUM']).any()  # actionNumber kept so shot detail joins on GAME_EVENT_ID
O.to_csv(dst,index=False); print('rows',len(O),'games',O.GAME_ID.nunique(),'unresolved subs',unres)
