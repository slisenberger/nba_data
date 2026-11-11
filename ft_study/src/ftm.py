import pandas as pd, subprocess, os, sys
rows=[]
for y in range(1996,2025):
    d=f'/home/claude/data/tmpf_{y}'; os.makedirs(d,exist_ok=True)
    subprocess.run(['tar','-xJf',f'/home/claude/shufinskiy/nba_data/datasets/nbastats_{y}.tar.xz','-C',d],check=True)
    x=pd.read_csv(f'{d}/nbastats_{y}.csv',usecols=['GAME_ID','EVENTMSGTYPE','EVENTMSGACTIONTYPE','HOMEDESCRIPTION','VISITORDESCRIPTION','PLAYER1_ID'],low_memory=False)
    subprocess.run(['rm','-rf',d])
    ft=x[(x.EVENTMSGTYPE==3)&~x.EVENTMSGACTIONTYPE.isin([16,21,22])]
    desc=ft.HOMEDESCRIPTION.fillna('')+ft.VISITORDESCRIPTION.fillna('')
    ft=ft.assign(made=~desc.str.contains('MISS'))
    g=ft.groupby('PLAYER1_ID').agg(fta=('made','size'),ftm=('made','sum'))
    gp=x[x.PLAYER1_ID>0].groupby('PLAYER1_ID').GAME_ID.nunique().rename('gp')
    g=g.join(gp,how='outer').fillna(0).reset_index().rename(columns={'PLAYER1_ID':'pid'}); g['season']=y; rows.append(g); print(y,flush=True)
pd.concat(rows).to_parquet('/home/claude/data/ftm.parquet')
