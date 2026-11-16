"""Ball-handling role features per player-season: assists and turnovers (raw nbastats PBP: assister = PLAYER2 on made
FGs, turnover = PLAYER1 on EVENTMSGTYPE 5; 2025-26 from the live feed: assistPersonId, turnover personId).
Output data/handling/handling_<y>.parquet: pid, season, ast, tov."""
import pandas as pd, numpy as np, subprocess, os, sys
y=int(sys.argv[1]); D='/home/claude/data'
tmp=f'{D}/tmp_h_{y}'; os.makedirs(tmp,exist_ok=True)
if y<=2024:
    subprocess.run(['tar','-xJf',f'/home/claude/shufinskiy/nba_data/datasets/nbastats_{y}.tar.xz','-C',tmp],check=True)
    f=[a for a in os.listdir(tmp) if a.endswith('.csv')][0]
    d=pd.read_csv(f'{tmp}/{f}',usecols=['EVENTMSGTYPE','PLAYER1_ID','PLAYER2_ID','PERSON1TYPE','PERSON2TYPE'],low_memory=False)
    ast=d[(d.EVENTMSGTYPE==1)&(d.PLAYER2_ID>0)&d.PERSON2TYPE.isin([4,5])].PLAYER2_ID.value_counts()
    tov=d[(d.EVENTMSGTYPE==5)&(d.PLAYER1_ID>0)&d.PERSON1TYPE.isin([4,5])].PLAYER1_ID.value_counts()
else:
    c=pd.read_csv(f'{D}/cdn25/cdnnba_2025.csv',usecols=['actionType','personId','assistPersonId','teamId'],low_memory=False)
    ast=c[c.assistPersonId.notna()].assistPersonId.astype('int64').value_counts()
    tov=c[(c.actionType=='turnover')&(c.teamId.notna())&(c.personId>0)].personId.astype('int64').value_counts()
subprocess.run(['rm','-rf',tmp])
o=pd.DataFrame({'ast':ast,'tov':tov}).fillna(0); o.index.name='pid'; o=o.reset_index(); o['season']=y
o.to_parquet(f'{D}/handling/handling_{y}.parquet'); print(y,len(o),int(o.ast.sum()),int(o.tov.sum()),flush=True)
