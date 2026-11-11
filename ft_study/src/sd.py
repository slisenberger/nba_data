"""Extract shot-detail (x/y, zone, action) per season to parquet, keyed by game_id + eventnum."""
import pandas as pd, subprocess, os, sys
y=int(sys.argv[1]); tag=sys.argv[2] if len(sys.argv)>2 else ''
d=f'/home/claude/data/tmpsd{tag}_{y}'; os.makedirs(d,exist_ok=True)
subprocess.run(['tar','-xJf',f'/home/claude/shufinskiy/nba_data/datasets/shotdetail{tag}_{y}.tar.xz','-C',d],check=True)
f=[x for x in os.listdir(d) if x.endswith('.csv')][0]
x=pd.read_csv(f'{d}/{f}',usecols=['GAME_ID','GAME_EVENT_ID','PLAYER_ID','LOC_X','LOC_Y','SHOT_DISTANCE','SHOT_ZONE_BASIC','SHOT_ZONE_AREA','ACTION_TYPE','SHOT_TYPE','SHOT_MADE_FLAG'],low_memory=False)
subprocess.run(['rm','-rf',d])
x=x.rename(columns={'GAME_ID':'game_id','GAME_EVENT_ID':'evnum','PLAYER_ID':'sd_pid','LOC_X':'loc_x','LOC_Y':'loc_y','SHOT_DISTANCE':'sd_dist','SHOT_ZONE_BASIC':'zone','SHOT_ZONE_AREA':'zone_area','ACTION_TYPE':'sd_action','SHOT_TYPE':'sd_type','SHOT_MADE_FLAG':'sd_made'})
x=x.drop_duplicates(['game_id','evnum'])
for c in ('zone','zone_area','sd_action','sd_type'): x[c]=x[c].astype('category')
x.to_parquet(f'/home/claude/data/sd/sd{tag}_{y}.parquet'); print(y,len(x),flush=True)
