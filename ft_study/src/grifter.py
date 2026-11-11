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
cols=['name','att_3PA_pre','att_3PA_post','f_3PA_pre','f_3PA_post','oe_3PA_pre','oe_3PA_post','d_3PA','z_3PA','att_MID_pre','att_MID_post','f_MID_pre','f_MID_post','oe_MID_pre','oe_MID_post','d_MID','lost_low_fouls']
print('league non-shooting drawn per off poss by season',lg.round(4).to_dict())
print(el.sort_values('d_3PA')[cols].head(25).round(2).to_string())
print(el.sort_values('lost_low_fouls',ascending=False)[cols].head(15).round(2).to_string())
print('corr(pre 3PA oe, change)',el[['oe_3PA_pre','d_3PA']].corr().iloc[0,1].round(3),' regression-to-mean benchmark: split-half below')
# regression to the mean: post-on-pre slope of 3PA fouls over expected, rule window vs several non-rule windows,
# with bootstrap SEs over players (audit: one placebo and no SE was too thin)
rng=np.random.default_rng(0)
def slope_win(pre_s,post_s):
    B=win(X.season.isin(pre_s),'pre').join(win(X.season.isin(post_s),'post'),how='inner').fillna(0)
    for t in ['pre','post']: B[f'oe_{t}']=100*(B[f'f_3PA_{t}']-B[f'x_3PA_{t}'])/B[f'att_3PA_{t}'].clip(lower=1)
    B=B[(B.att_3PA_pre>=300)&(B.att_3PA_post>=300)]
    x=B.oe_pre.to_numpy(); y=B.oe_post.to_numpy(); b=np.polyfit(x,y,1)[0]
    bs=[np.polyfit(x[i],y[i],1)[0] for i in (rng.integers(0,len(x),len(x)) for _ in range(1000))]
    return b,np.std(bs),len(x)
SL=[]
for lab,pre_s,post_s in [('rule change 2019-21 -> 2021-23',[2019,2020],[2021,2022]),('placebo 2012-14 -> 2014-16',[2012,2013],[2014,2015]),
                         ('placebo 2014-16 -> 2016-18',[2014,2015],[2016,2017]),('placebo 2016-18 -> 2018-20',[2016,2017],[2018,2019])]:
    b,se,nn=slope_win(pre_s,post_s); SL.append(dict(window=lab,slope=b,se=se,players=nn)); print(lab,'slope',round(b,3),'+-',round(se,3),'n',nn)
SL=pd.DataFrame(SL); SL.round(4).to_csv(f'{OUT}/grifter_slopes.csv',index=False)
pl=SL.iloc[1:]; d=SL.slope.iloc[0]-pl.slope.mean(); dse=np.sqrt(SL.se.iloc[0]**2+(pl.se**2).mean())
print('rule slope minus mean placebo slope',round(d,3),'+-',round(dse,3))
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
