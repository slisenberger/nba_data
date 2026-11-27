"""Turnovers per player-game from raw nbastats PBP (EVENTMSGTYPE 5, PLAYER1 a player). Regular season or playoffs.
Usage: tov_games.py <year> [po]. Output data/tov/tov[_po]_<y>.parquet: game_id, pid, tov."""
import pandas as pd, subprocess, os, sys
y=int(sys.argv[1]); tag='_po' if len(sys.argv)>2 and sys.argv[2]=='po' else ''
D='/home/claude/data'; tmp=f'{D}/tmp_tov{tag}_{y}'; os.makedirs(tmp,exist_ok=True)
subprocess.run(['tar','-xJf',f'/home/claude/shufinskiy/nba_data/datasets/nbastats{tag}_{y}.tar.xz','-C',tmp],check=True)
f=[a for a in os.listdir(tmp) if a.endswith('.csv')][0]
d=pd.read_csv(f'{tmp}/{f}',usecols=['GAME_ID','EVENTMSGTYPE','PLAYER1_ID','PERSON1TYPE'],low_memory=False); subprocess.run(['rm','-rf',tmp])
t=d[(d.EVENTMSGTYPE==5)&(d.PLAYER1_ID>0)&d.PERSON1TYPE.isin([4,5])].groupby(['GAME_ID','PLAYER1_ID']).size().rename('tov').reset_index()
t.columns=['game_id','pid','tov']; t.to_parquet(f'{D}/tov/tov{tag}_{y}.parquet'); print(y,tag,len(t),int(t.tov.sum()),flush=True)
