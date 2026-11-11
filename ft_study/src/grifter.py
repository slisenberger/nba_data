"""Grifter identification (proposed work item 4).
(a) Natural experiment: the 2021-22 'non-basketball move' rule (leg kicks, leaning into defenders, launching on
    3s, rip-throughs). For each player with enough attempts in both windows, compare fouls drawn above xFoul
    (xF has zone x season FEs, so the league-wide drop is in the expectation) on 3PA and on 2PT jump zones
    (mid-range), pre (2019-20, 2020-21) vs post (2021-22, 2022-23). Also non-shooting fouls drawn per 100 on-court
    offensive possessions (rip-throughs on the floor), 2005+ only (drawer named).
(b) Career signal: fouls above xF on low-xFoul shots (3PA + mid-range), per 100 such attempts, shrunk."""
import pandas as pd, numpy as np, glob
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
X=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['season','shooter','z','w','is3','fouled','xf','att_id','kind'])
names=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'{D}/out/names_*.parquet'))]).drop_duplicates('pid').set_index('pid').name
X['grp']=np.where(X.is3==1,'3PA',np.where(X.z=='MID','MID','RIM_PAINT'))
X['f']=X.fouled*X.w; X['x']=X.xf*X.w
pre=X.season.isin([2019,2020]); post=X.season.isin([2021,2022])
def win(m,lab):
    g=X[m].groupby(['shooter','grp']).agg(att=('w','sum'),f=('f','sum'),x=('x','sum')).unstack('grp')
    g.columns=[f'{a}_{b}_{lab}' for a,b in g.columns]; return g
W=win(pre,'pre').join(win(post,'post'),how='inner').fillna(0)
# non-shooting drawn per 100 off poss
rows=[]
for y in [2019,2020,2021,2022]:
    F=pd.read_parquet(f'{D}/out/fouls_{y}.parquet',columns=['drawn_by','action','pen_before'])
    P=pd.read_parquet(f'{D}/cf/cf_players_{y}.parquet',columns=['pid','off_poss'])
    ns=F[(F.drawn_by>0)&~F.action.isin([2,29])].drawn_by.value_counts().rename('ns')
    P=P.join(ns,on='pid').fillna(0); P['season']=y; rows.append(P)
NS=pd.concat(rows); NS['win']=np.where(NS.season<=2020,'pre','post')
lg=NS.groupby('season').apply(lambda g: g.ns.sum()/g.off_poss.sum())
NS['xns']=NS.off_poss*NS.season.map(lg)
N2=NS.groupby(['pid','win'])[['ns','xns','off_poss']].sum().unstack('win'); N2.columns=[f'{a}_{b}' for a,b in N2.columns]
W=W.join(N2,how='left')
for g in ['3PA','MID','RIM_PAINT']:
    for t in ['pre','post']:
        W[f'oe_{g}_{t}']=100*(W[f'f_{g}_{t}']-W[f'x_{g}_{t}'])/W[f'att_{g}_{t}'].clip(lower=1)   # fouls over expected per 100 att
    W[f'd_{g}']=W[f'oe_{g}_post']-W[f'oe_{g}_pre']
    # binomial-ish SE of the change
    W[f'se_{g}']=100*np.sqrt(W[f'x_{g}_pre']/W[f'att_{g}_pre'].clip(lower=1)**2+W[f'x_{g}_post']/W[f'att_{g}_post'].clip(lower=1)**2)
W['ns100_pre']=100*W.ns_pre/W.off_poss_pre; W['ns100_post']=100*W.ns_post/W.off_poss_post
W['ns_oe_pre']=100*(W.ns_pre-W.xns_pre)/W.off_poss_pre; W['ns_oe_post']=100*(W.ns_post-W.xns_post)/W.off_poss_post
W['d_ns']=W.ns_oe_post-W.ns_oe_pre
# lost fouls per 100 shooting attempts, all low-xF shots (3PA+MID), scaled to the post attempt volume
W['att_low_post']=W['att_3PA_post']+W['att_MID_post']
W['lost_low_fouls']=-(W.d_3PA*W['att_3PA_post']+W.d_MID*W['att_MID_post'])/100
W['name']=W.index.map(names)
el=W[(W['att_3PA_pre']>=300)&(W['att_3PA_post']>=300)].copy()
el['z_3PA']=el.d_3PA/el.se_3PA
el.reset_index().rename(columns={'shooter':'pid'}).round(4).to_csv(f'{OUT}/grifter_rule_change.csv',index=False)
pd.set_option('display.width',250)
cols=['name','att_3PA_pre','att_3PA_post','oe_3PA_pre','oe_3PA_post','d_3PA','z_3PA','oe_MID_pre','oe_MID_post','d_MID','ns100_pre','ns100_post','d_ns','lost_low_fouls']
print('league non-shooting drawn per off poss by season',lg.round(4).to_dict())
print(el.sort_values('d_3PA')[cols].head(25).round(2).to_string())
print(el.sort_values('lost_low_fouls',ascending=False)[cols].head(15).round(2).to_string())
print('corr(pre 3PA oe, change)',el[['oe_3PA_pre','d_3PA']].corr().iloc[0,1].round(3),' regression-to-mean benchmark: split-half below')
# regression to the mean benchmark: same comparison across a non-rule boundary (2017-18 vs 2018-19)
pa=X.season.isin([2016,2017]); pb=X.season.isin([2018,2019])
B=win(pa,'pre').join(win(pb,'post'),how='inner').fillna(0)
for t in ['pre','post']: B[f'oe_{t}']=100*(B[f'f_3PA_{t}']-B[f'x_3PA_{t}'])/B[f'att_3PA_{t}'].clip(lower=1)
Bel=B[(B.att_3PA_pre>=300)&(B.att_3PA_post>=300)]
sl=lambda d: np.polyfit(d.oe_pre,d.oe_post,1)[0]
print('slope of post on pre 3PA fouls-over-expected: placebo 2016-17->2018-19',round(sl(Bel),3),' rule change',round(np.polyfit(el.oe_3PA_pre,el.oe_3PA_post,1)[0],3))
# (b) career low-xF foul drawing
C=X[X.grp.isin(['3PA','MID'])].groupby(['shooter','grp']).agg(att=('w','sum'),f=('f','sum'),x=('x','sum')).unstack('grp').fillna(0)
C.columns=[f'{a}_{b}' for a,b in C.columns]
k=400
for g in ['3PA','MID']:
    C[f'oe100_{g}']=100*(C[f'f_{g}']-C[f'x_{g}'])/(C[f'att_{g}']+k)    # shrunk toward 0 with k pseudo-attempts
C['low_att']=C.att_3PA+C.att_MID; C['low_oe']=C.f_3PA+C.f_MID-C.x_3PA-C.x_MID
C['low_oe100']=100*C.low_oe/(C.low_att+k)
fy=X.groupby('shooter').season.agg(first='min',last='max'); C=C.join(fy); C['name']=C.index.map(names)
C=C[C.low_att>=2000]
C.reset_index().rename(columns={'shooter':'pid'}).round(4).to_csv(f'{OUT}/grifter_lowxf_career.csv',index=False)
print(C.sort_values('low_oe100',ascending=False)[['name','first','last','att_3PA','att_MID','f_3PA','x_3PA','oe100_3PA','oe100_MID','low_oe','low_oe100']].head(25).round(2).to_string())
