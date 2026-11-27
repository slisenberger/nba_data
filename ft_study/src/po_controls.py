"""Playoff TS gap with environmental controls (each season vs its own RS, minimal model: league drop + talent):
 1) garbage time removed (Q4/OT possessions with |margin| >= 15) from every segment;
 2) transition mix: expected TS from the player's share of transition attempts (<= 7 s, after a live-ball miss or steal),
    using the segment league's TS in transition vs half-court; subtracted as an environment term;
 3) free-throw luck: free throws counted at the player's season FT% (all games) instead of actual makes;
 4) home share: (home share - 0.5) x the season's league home-away TS difference.
Also: playoff rTS by game number within a series (in-series adjustment), relative to the player's series average.
Persistence vs placebo for each control added cumulatively; named players."""
import pandas as pd, numpy as np
from scipy.stats import chi2
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'; SD1=0.575
rows=[]; series=[]
for y in range(1996,2025):
    po=pd.read_parquet(f'{D}/out_po/att_{y}.parquet',columns=['game_id','off','shooter','kind','pts','fta','ftm','margin','period','start_type','elapsed']); pot=set(po.off.unique())
    rs=pd.read_parquet(f'{D}/out/att_{y}.parquet',columns=['game_id','off','deff','shooter','kind','pts','fta','ftm','margin','period','start_type','elapsed'])
    hm=pd.concat([pd.read_parquet(f'{D}/{s}/poss_{y}.parquet',columns=['game_id','off','home_off']) for s in ('out','out_po')]); hm=hm[hm.home_off==1].drop_duplicates('game_id').set_index('game_id').off
    rs['seg']=np.where(rs.deff.isin(pot),'pl',np.where(rs.game_id%2==1,'npA','npB')); po['seg']='po'
    a=pd.concat([rs.drop(columns='deff'),po],ignore_index=True); a=a[a.shooter>0]
    a['home']=(a.off==a.game_id.map(hm)).astype(float)
    a['garb']=(a.period>=4)&(a.margin.abs()>=15)
    a['trans']=(a.elapsed.between(0,7))&a.start_type.isin(['miss','steal'])
    a['fga']=a.kind.isin(['fga','and1']).astype(float); a['tsa']=a.fga+0.44*a.fta
    ftp=a[a.fta>0].groupby('shooter').apply(lambda g: (g.ftm.sum()+5*0.76)/(g.fta.sum()+5))   # season FT%, lightly shrunk
    a['pts_ft']=a.pts-a.ftm+a.fta*a.shooter.map(ftp).fillna(0.76)
    a['segg']=np.where(a.seg.isin(['npA','npB']),'np',a.seg)
    for lab,sub in [('all',a),('nogarb',a[~a.garb])]:
        L=sub.groupby('segg').apply(lambda g: pd.Series({'lg':g.pts.sum()/(2*g.tsa.sum()),'lgft':g.pts_ft.sum()/(2*g.tsa.sum()),
            'l_tr':g[g.trans].pts.sum()/(2*g[g.trans].tsa.sum()),'l_hc':g[~g.trans].pts.sum()/(2*g[~g.trans].tsa.sum()),
            'hadv':g[g.home==1].pts.sum()/(2*g[g.home==1].tsa.sum())-g[g.home==0].pts.sum()/(2*g[g.home==0].tsa.sum())}))
        g=sub.assign(tt=sub.tsa*sub.trans,th=sub.tsa*sub.home).groupby(['shooter','seg']).agg(pts=('pts','sum'),pts_ft=('pts_ft','sum'),tsa=('tsa','sum'),tt=('tt','sum'),th=('th','sum')).reset_index()
        g['segg']=np.where(g.seg.isin(['npA','npB']),'np',g.seg); g=g.join(L,on='segg'); g['season']=y; g['ver']=lab; rows.append(g)
    # in-series: playoff game number = last digit of game id
    p2=po[po.shooter>0].copy(); p2['tsa']=p2.kind.isin(['fga','and1']).astype(float)+0.44*p2.fta; p2['gn']=p2.game_id%10; p2['ser']=p2.game_id//10
    s=p2.groupby(['shooter','ser','gn']).agg(pts=('pts','sum'),tsa=('tsa','sum')).reset_index(); series.append(s)
G=pd.concat(rows)
G['share_t']=G.tt/G.tsa; G['share_h']=G.th/G.tsa
G['rts_raw']=100*(G.pts/(2*G.tsa)-G.lg)
G['env_t']=100*(G.share_t*G['l_tr']+(1-G.share_t)*G['l_hc']-G.lg)          # expected TS from transition mix, vs league
G['env_h']=100*(G.share_h-0.5)*G.hadv
G['rts_ft']=100*(G.pts_ft/(2*G.tsa)-G.lgft)
def build(ver,col,env):
    H=G[G.ver==ver].copy(); H['y_']=H[col]-sum(H[e] for e in env) if env else H[col]
    W=H.pivot_table(index=['shooter','season'],columns='seg',values=['y_','tsa']); W.columns=[a+'_'+b for a,b in W.columns]; W=W.reset_index()
    W=W[(W.tsa_po>=50)&(W.tsa_pl>=100)&(W.tsa_npA>=50)&(W.tsa_npB>=50)].copy(); W['np_']=(W.y__npA*W.tsa_npA+W.y__npB*W.tsa_npB)/(W.tsa_npA+W.tsa_npB)
    W['gap']=W.y__po-W.y__pl; W['v']=(100*SD1)**2*(1/W.tsa_po+1/W.tsa_pl); W['gp']=W.y__pl-W.y__npA; W['vp']=(100*SD1)**2*(1/W.tsa_pl+1/W.tsa_npA)
    for c,x,v in [('gap','np_','v'),('gp','y__npB','vp')]:
        X=np.column_stack([np.ones(len(W)),W[x]]); w=1/W[v].to_numpy(); sw=np.sqrt(w); b=np.linalg.lstsq(X*sw[:,None],W[c].to_numpy()*sw,rcond=None)[0]; W['r_'+c]=W[c].to_numpy()-X@b
        if c=='gap': W.attrs['drop']=b[0]
    return W
nm=pd.read_csv(f'{OUT}/reaves_seasons.csv')[['shooter','name']].drop_duplicates('shooter').set_index('shooter').name
def persist(W,col,v,k=4):
    p=W.groupby('shooter').apply(lambda g: pd.Series({'m':np.average(g[col],weights=1/g[v]),'v':1/np.sum(1/g[v]),'k':len(g)})); p=p[p.k>=k]
    def Qt(t2): w=1/(p.v+t2); mb=np.sum(w*p.m)/w.sum(); return np.sum(w*(p.m-mb)**2)
    Q=Qt(0); df=len(p)-1; w=1/p.v; tau2=max(0,(Q-df)/(w.sum()-np.sum(w**2)/w.sum())); return p,Q,df,1-chi2.cdf(Q,df),np.sqrt(tau2)
specs=[('baseline (as before)','all','rts_raw',[]),('+ garbage time removed','nogarb','rts_raw',[]),('+ transition mix','nogarb','rts_raw',['env_t']),
       ('+ free-throw luck removed','nogarb','rts_ft',['env_t']),('+ home share','nogarb','rts_ft',['env_t','env_h'])]
res={}
print('TS points; league-wide drop, persistence (players >=4 runs), placebo')
for lab,ver,col,env in specs:
    W=build(ver,col,env); p,Q,df,pv,tau=persist(W,'r_gap','v'); _,Qp,dfp,pvp,taup=persist(W,'r_gp','vp')
    print(f'  {lab:28s} drop {W.attrs["drop"]:+.2f} | SD {tau:.2f} p {pv:.3f} | placebo SD {taup:.2f} p {pvp:.3f}'); res[lab]=(p,W)
# environment shifts league-wide
H=G[G.ver=='nogarb']; print('\nplayoffs vs RS-vs-playoff-teams, attempt-weighted: transition share',round(np.average(H[H.seg=='po'].share_t,weights=H[H.seg=='po'].tsa),3),'vs',round(np.average(H[H.seg=='pl'].share_t,weights=H[H.seg=='pl'].tsa),3),
      '| transition TS premium (pts)',round(100*np.average((H['l_tr']-H['l_hc'])[H.seg=='pl'],weights=H[H.seg=='pl'].tsa),1))
p0,_=res['baseline (as before)']; p1,W1=res['+ home share']
J=p0[['m']].rename(columns={'m':'baseline'}).join(p1[['m','v','k']].rename(columns={'m':'all_controls'})); J['se']=np.sqrt(J.v); J['name']=J.index.map(nm)
ch=['James Harden','John Stockton','Karl Malone','Reggie Miller','LeBron James','Kawhi Leonard','Dirk Nowitzki','Nikola Jokic','Kobe Bryant','Tim Duncan','Chris Paul','Kevin Durant','Stephen Curry','Giannis Antetokounmpo','Joel Embiid']
print('\nnamed (TS points after league drop): baseline -> all controls'); print(J[J.name.isin(ch)].sort_values('all_controls')[['name','k','baseline','all_controls','se']].round(2).to_string(index=False))
print('\nbiggest movers'); J['chg']=J.all_controls-J.baseline; print(J.reindex(J.chg.abs().sort_values(ascending=False).index)[['name','k','baseline','all_controls']].head(8).round(2).to_string(index=False))
J.round(3).to_csv(f'{OUT}/po_controls_players.csv'); W1.round(3).to_csv(f'{OUT}/po_controls_seasons.csv',index=False)
# in-series adjustment
S=pd.concat(series); S['ts']=S.pts/(2*S.tsa); sm=S.groupby(['shooter','ser']).apply(lambda g: g.pts.sum()/(2*g.tsa.sum())).rename('sts')
S=S.join(sm,on=['shooter','ser']); S=S[S.tsa>=5]; S['d']=100*(S.ts-S.sts)
print('\nin-series: player rTS by game number vs his series average (TS pts, attempt-weighted)')
print(S.groupby('gn').apply(lambda g: pd.Series({'n':len(g),'d':np.average(g.d,weights=g.tsa)})).round(2).T.to_string())
