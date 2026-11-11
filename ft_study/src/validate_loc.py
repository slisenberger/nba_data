"""Holdout check of the fouled-miss location imputation: on 2016-17+ (logged foul locations), compare REAVES built
from logged locations (main run) with REAVES from the imputation alone (FORCE_IMPUTE=1 run, kappas calibrated on the
logged seasons), at the player-season and player (2016+ total) level; also the old (v1, 3PT-kappa) imputation."""
import pandas as pd, numpy as np
R='/home/user/nba_data/ft_study/results'
a=pd.read_csv(f'{R}/reaves_seasons.csv'); b=pd.read_csv(f'{R}/reaves_seasons_imputed.csv')
m=a.merge(b,on=['shooter','season'],suffixes=('','_imp')); m=m[(m.season>=2016)&(m.att>=300)]
out=[]
def comp(lbl,x,y):
    d=y-x; out.append(dict(level=lbl,n=len(x),corr=np.corrcoef(x,y)[0,1],mae=np.abs(d).mean(),bias=d.mean(),sd_true=x.std()))
for col in ['xf','reaves']:
    comp(f'player-season {col}',m[col],m[col+'_imp'])
c=m.groupby('shooter')[['reaves','reaves_imp','xf','xf_imp','att']].sum(); c=c[c.att>=3000]
comp('player 2016+ reaves',c.reaves,c.reaves_imp)
V=pd.DataFrame(out); V.to_csv(f'{R}/loc_validation.csv',index=False); print(V.round(3).to_string())
names=a.drop_duplicates('shooter').set_index('shooter').name
c['name']=c.index.map(names); c['diff']=c.reaves_imp-c.reaves
print(c.sort_values('diff')[['name','att','reaves','reaves_imp','diff']].head(8).round(1).to_string())
print(c.sort_values('diff')[['name','att','reaves','reaves_imp','diff']].tail(8).round(1).to_string())
