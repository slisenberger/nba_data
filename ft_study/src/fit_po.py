"""Playoff-only xFoul: the same FE specification as xfoul.py, fit on playoff attempts alone (xfoul_po_cells.parquet), so a
player's playoff fouls - xF_po is measured against playoff norms (by construction the playoffs sum to zero). Compares
playoff REAVES (playoff model) with regular-season REAVES and with the RS-model playoff numbers (xfoul_att_po)."""
import pandas as pd, numpy as np
from fe import fe_resid
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
C=pd.read_parquet(f'{D}/xfoul_po_cells.parquet'); P=pd.read_parquet(f'{D}/xfoul_att_po.parquet')
assert len(C)==len(P)
fes=['f_zs','f_el','f_cl','f_mb','f_pen','f_dts','f_dfr','f_pb']
C['xf_po']=C.fouled-fe_resid(C,'fouled',fes,w='w0',tol=1e-9)
P['xf_po']=C.xf_po.clip(0.001,0.999).to_numpy()
P.to_parquet(f'{D}/xfoul_att_po.parquet')
w=P.w
for lab,c in [('RS model','xf'),('playoff model','xf_po')]:
    print(lab,' fouls/xF',round((P.fouled*w).sum()/(P[c]*w).sum(),4))
Z=P.assign(fw=P.fouled*w,a=P.xf*w,b=P.xf_po*w).groupby('z')[['w','fw','a','b']].sum(); Z['rate_obs']=Z.fw/Z.w; Z['rate_RSmodel']=Z.a/Z.w; Z['rate_POmodel']=Z.b/Z.w
print(Z.round(4).to_string())
P['r_rs']=(P.fouled-P.xf)*P.dv*w; P['r_po']=(P.fouled-P.xf_po)*P.dv*w
G=P.groupby('shooter').agg(att=('w','sum'),f=('f_w','sum'),xf_rs=('xf_w','sum'),r_rs=('r_rs','sum'),r_po=('r_po','sum'),runs=('season','nunique'))
G['xf_po']=P.assign(t=P.xf_po*w).groupby('shooter').t.sum(); G['foe_po']=G.f/G.xf_po
R=pd.read_csv(f'{OUT}/reaves_career.csv').set_index('shooter')[['name','reaves_per100','foe','att']].rename(columns={'att':'rs_att','foe':'foe_rs','reaves_per100':'rs_per100'})
G=G.join(R,how='inner'); G['po_per100']=100*G.r_po/G.att; G['po_per100_rsmodel']=100*G.r_rs/G.att
H=G[G.att>=1000]
print('players with >=1000 playoff attempts',len(H))
print('corr RS REAVES/100 vs playoff REAVES/100 (playoff model)',round(H.rs_per100.corr(H.po_per100),3),'  slope',round(np.polyfit(H.rs_per100,H.po_per100,1)[0],3))
c=['name','runs','att','f','xf_po','foe_po','po_per100','foe_rs','rs_per100']
print('TOP playoff REAVES per 100 attempts (playoff model)'); print(H.sort_values('po_per100',ascending=False)[c].head(15).round(2).to_string(index=False))
print('BOTTOM'); print(H.sort_values('po_per100')[c].head(8).round(2).to_string(index=False))
print('TOP total'); print(H.sort_values('r_po',ascending=False)[['name','runs','r_po','po_per100']].head(10).round(1).to_string(index=False))
G.round(4).to_csv(f'{OUT}/reaves_playoffs_career.csv')
