"""Player height and weight for the size-adjusted REAVES. Source: Basketball-Reference's player index (one page per
letter; fetched once into data/ext/bbref/, re-fetched only if missing, 4 s apart). Matched to our player ids by
normalized name + career overlap (bbref years are season END years). Output: data/ext/heights.csv and
results/player_heights_bbref.csv."""
import re, glob, os, time, html, subprocess, unicodedata, pandas as pd, numpy as np
D='/home/claude/data'; E=f'{D}/ext/bbref'; os.makedirs(E,exist_ok=True)
for l in 'abcdefghijklmnopqrstuvwxyz':
    f=f'{E}/{l}.html'
    if not os.path.exists(f) or os.path.getsize(f)<50000:
        subprocess.run(['curl','-s','-m','30','-A','Mozilla/5.0 (research script)','-o',f,f'https://www.basketball-reference.com/players/{l}/']); time.sleep(4)
rows=[]
for f in sorted(glob.glob(f'{E}/*.html')):
    t=open(f,encoding='utf-8',errors='ignore').read()
    for tr in re.findall(r'<tr ?>(.*?)</tr>',t,re.S):
        nm=re.search(r'data-stat="player"[^>]*>(?:<strong>)?<a href="([^"]+)">([^<]+)</a>',tr)
        if not nm: continue
        g=lambda k: (re.search(rf'data-stat="{k}"[^>]*>([^<]*)<',tr) or [None,None])[1]
        rows.append(dict(bbref=nm.group(1),name=html.unescape(nm.group(2)),ymin=g('year_min'),ymax=g('year_max'),pos=g('pos'),ht=g('height'),wt=g('weight')))
B=pd.DataFrame(rows)
def inch(s):
    try: a,b=s.split('-'); return int(a)*12+int(b)
    except Exception: return np.nan
B['height_in']=B.ht.map(inch); B['weight']=pd.to_numeric(B.wt,errors='coerce'); B['ymin']=pd.to_numeric(B.ymin); B['ymax']=pd.to_numeric(B.ymax)
norm=lambda x: re.sub(r'[^a-z ]','',unicodedata.normalize('NFKD',str(x)).encode('ascii','ignore').decode().lower().replace('.','').replace(' jr','').replace(' sr','').replace(' iii','').replace(' ii','').replace(' iv','')).strip()
B['key']=B.name.map(norm); B=B[B.ymax>=1997]
names=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'{D}/out/names_*.parquet'))]).drop_duplicates('pid')
yrs=pd.concat([pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['shooter']).drop_duplicates().assign(season=y) for y in range(1996,2026)])
P=yrs.groupby('shooter').season.agg(first='min',last='max').reset_index().merge(names.rename(columns={'pid':'shooter'}),on='shooter')
P['key']=P.name.map(norm)
# 2025-26 rookies are named "R. Nembhard" style: match initial + last name, still requiring career overlap
P['key2']=P.name.map(lambda x: norm(x).split(' ')[-1] if re.match(r'^[A-Z]\. ',str(x)) else None)
P['ini']=P.name.map(lambda x: str(x)[0].lower() if re.match(r'^[A-Z]\. ',str(x)) else None)
B['key2']=B.key.str.split(' ').str[-1]; B['ini']=B.key.str[0]
m1=P.merge(B[['key','ymin','ymax','height_in','weight','pos']],on='key',how='inner')
m2=P[P.key2.notna()].merge(B[['key2','ini','ymin','ymax','height_in','weight','pos']],on=['key2','ini'],how='inner')
m=pd.concat([m1,m2]); m=m[(m.ymin<=m['last']+1)&(m.ymax>=m['first']+1)].sort_values('ymin').drop_duplicates('shooter')
out=m[['shooter','name','height_in','weight','pos']]
out.to_csv(f'{D}/ext/heights.csv',index=False); out.to_csv('/home/user/nba_data/ft_study/results/player_heights_bbref.csv',index=False)
A=pd.concat([pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['shooter','kind']) for y in range(1996,2026)]); A=A[A.kind.isin(['fga','and1','fouled_miss'])]
print('players matched',len(out),'of',len(P),' share of attempts',round(A.shooter.isin(out.shooter).mean(),4))
