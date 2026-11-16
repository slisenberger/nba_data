"""Score playoff shooting attempts with the REGULAR-SEASON xFoul model (fit on RS only), so playoff fouls - xF isolates the
playoff whistle at a fixed shot zone and context.
Method: RS stage-1 rows (xfoul_stage1.parquet) + playoff rows with weight 0 go through the same FE projection; zero-weight rows
don't move any group mean, and get the RS model's fitted value for their cells. RS fitted values are checked against the
stored ones. Inputs for playoff rows follow xfoul.py: shot zones from playoff shot detail (sd_po), fouled-miss zones from
logged playoff foul spots 2016+ (floc_att_po, RS and-one confusion matrix) or else the player-season's RS fouled-miss zone
mix, pre-foul team-foul state restored, defenders' foul propensity from RS rates with RS decile edges, referee crew = sum of
the refs' RS ridge effects, dv (value of a fouled vs unfouled attempt) = RS season x zone value.
Output data/xfoul_att_po.parquet."""
import pandas as pd, numpy as np, glob, gc, json
import pyarrow.parquet as pq
from fe import fe_resid
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
Z2=['RA','PAINT','MID']; Z3=['C3','AB3']
zmap={'Restricted Area':'RA','In The Paint (Non-RA)':'PAINT','Mid-Range':'MID','Left Corner 3':'C3','Right Corner 3':'C3','Above the Break 3':'AB3','Backcourt':'AB3'}
cols=['game_id','season','evnum','shooter','kind','made','is3','dist','period','clk','elapsed','start_type','margin','def_tf','def_pen','deff','off','fouled','fouler','ftm','fta','pts','since_oreb','pidx']
def dfr_rates(s):
    tb=pq.read_table(f'{D}/out/poss_{s}.parquet',columns=['def_lu']).column('def_lu').combine_chunks()
    v=tb.flatten().to_numpy(); u,c=np.unique(v,return_counts=True)
    Fo=pd.read_parquet(f'{D}/out/fouls_{s}.parquet',columns=['fouler','action']); sf=Fo[Fo.action.isin([2,29])].fouler.value_counts()
    k0=sf.sum()/max(c.sum(),1); return pd.Series((sf.reindex(u).fillna(0).to_numpy()+200*k0)/(c+200),index=u)
parts=[]
for y in range(1996,2025):
    f=f'{D}/out_po/att_{y}.parquet'; a=pd.read_parquet(f,columns=cols)
    lu=pq.read_table(f,columns=['def_lu']).column('def_lu').combine_chunks(); off=lu.offsets.to_numpy(); vals=lu.flatten().to_numpy()
    rv=dfr_rates(y).reindex(vals).to_numpy()
    sums=np.add.reduceat(np.nan_to_num(rv),off[:-1]) if len(vals) else np.zeros(len(a)); cnt=np.add.reduceat(~np.isnan(rv),off[:-1]).astype(float)
    ln=np.diff(off); sums=np.where(ln>0,sums,0); cnt=np.where(ln>0,cnt,0)
    a['dfr']=np.where(cnt>0,sums/np.maximum(cnt,1),np.nan)
    a=a[a.kind.isin(['fga','and1','fouled_miss'])&(a.shooter>0)]
    sd=pd.read_parquet(f'{D}/sd/sd_po_{y}.parquet',columns=['game_id','evnum','zone','sd_action'])
    a=a.merge(sd,on=['game_id','evnum'],how='left'); parts.append(a)
A=pd.concat(parts,ignore_index=True); del parts
z=A.zone.astype(object).map(zmap); fb=np.select([A.is3==1,A.dist<0,A.dist<=4,A.dist<=9],['AB3','MID','RA','PAINT'],'MID'); z=z.where(z.notna(),fb)
z=np.where((A.is3==1)&~pd.Series(z).isin(Z3),'AB3',z); z=np.where((A.is3==0)&pd.Series(z).isin(Z3),'MID',z)
A['z']=np.where(A.kind=='fouled_miss','?',z)
print('playoff attempts',len(A),' shot-detail match (FGA)',round(A.loc[A.kind!='fouled_miss','zone'].notna().mean(),4),flush=True)
# pre-foul team-foul state for fouled misses
Fo=pd.concat([pd.read_parquet(f,columns=['game_id','period','clk','fouler','tf_after','pen_before']) for f in sorted(glob.glob(f'{D}/out_po/fouls_*.parquet'))])
Fo['c10']=(Fo.clk*10).round().astype(np.int64); Fo=Fo.drop_duplicates(['game_id','period','c10','fouler'])
A['c10']=(A.clk.astype(np.float64)*10).round().astype(np.int64); ii=A.index[A.kind=='fouled_miss']
mm=A.loc[ii,['game_id','period','c10','fouler']].merge(Fo[['game_id','period','c10','fouler','tf_after','pen_before']],on=['game_id','period','c10','fouler'],how='left')
ok=mm.tf_after.notna().to_numpy(); A.loc[ii[ok],'def_tf']=mm.tf_after.to_numpy()[ok]-1; A.loc[ii[ok],'def_pen']=mm.pen_before.to_numpy()[ok]
print('fouled misses with pre-foul state restored',round(ok.mean(),4),flush=True)
# fouled-miss zones
LOC=pd.read_parquet(f'{D}/floc_att_po.parquet',columns=['game_id','att_evnum','kind','fz','loc_ok'])
lf=LOC[(LOC.kind=='fouled_miss')&LOC.loc_ok][['game_id','att_evnum','fz']].rename(columns={'att_evnum':'evnum'}).drop_duplicates(['game_id','evnum'])
A=A.merge(lf,on=['game_id','evnum'],how='left')
CONF=pd.read_csv(f'{OUT}/floc_confusion_and1.csv',index_col=0).reindex(index=Z2+Z3,columns=Z2+Z3,fill_value=0).astype(float)
XR=pd.read_parquet(f'{D}/xfoul_att.parquet',columns=['season','shooter','kind','is3','z','w','dv'])
fmr=XR[XR.kind=='fouled_miss'].groupby(['shooter','season','is3','z']).w.sum()
mix=(fmr/fmr.groupby(level=[0,1,2]).transform('sum')).rename('wz').reset_index()
lmix=XR[XR.kind=='fouled_miss'].groupby(['season','is3','z']).w.sum(); lmix=(lmix/lmix.groupby(level=[0,1]).transform('sum')).rename('wz').reset_index()
DV=XR.groupby(['season','z']).dv.first().rename('dv').reset_index(); del XR; gc.collect()
fm=A[A.kind=='fouled_miss'].copy(); fm['row']=fm.index
lg=fm[fm.fz.notna()]; rows=[]
for r in lg[['row','fz','is3']].itertuples():
    zs=Z3 if r.is3 else Z2; v=CONF.loc[r.fz,zs].to_numpy()+1e-9
    if v.sum()>1e-6:
        for zz,ww in zip(zs,v/v.sum()):
            if ww>1e-6: rows.append((r.row,zz,ww,1))
done=set(x[0] for x in rows)
im=fm[~fm.row.isin(done)][['row','shooter','season','is3']]
p1=im.merge(mix,on=['shooter','season','is3'],how='inner'); got=set(p1.row)
p2=im[~im.row.isin(got)].merge(lmix,on=['season','is3'],how='inner')
EX=pd.concat([pd.DataFrame(rows,columns=['row','z','wz','loc_logged']),p1[['row','z','wz']].assign(loc_logged=0),p2[['row','z','wz']].assign(loc_logged=0)],ignore_index=True)
print('fouled misses: logged',len(done),' player RS mix',len(got),' league mix',p2.row.nunique(),' of',len(fm),flush=True)
FMX=fm.drop(columns=['z']).loc[EX.row].reset_index(drop=True); FMX['z']=EX.z.values; FMX['w0']=EX.wz.values; FMX['loc_logged']=EX.loc_logged.values
FG=A[A.kind!='fouled_miss'].copy(); FG['w0']=1.0; FG['loc_logged']=1
P=pd.concat([FG,FMX],ignore_index=True); del FG,FMX,fm,A; gc.collect()
P['att_id']=np.r_[np.arange((P.kind!='fouled_miss').sum()),(P.kind!='fouled_miss').sum()+pd.factorize(P[P.kind=='fouled_miss'].index)[0]] if False else 0
# ---- RS rows for the projection
need=['game_id','season','z','w','fouled','elapsed','start_type','period','clk','margin','def_pen','deff','is3','dfr','since_oreb','xf_base']
R=pd.read_parquet(f'{D}/xfoul_stage1.parquet',columns=need)
# RS decile edges of the defenders' propensity
edges={s:np.nanquantile(g.dfr.to_numpy(),np.arange(1,10)/10) for s,g in R.groupby('season')}
P['dfr_bin']=[(-1 if np.isnan(v) else int(np.searchsorted(edges[s],v,side='right'))) for v,s in zip(P.dfr.astype(float),P.season)]
R['dfr_bin']=pd.read_parquet(f'{D}/xfoul_stage1.parquet',columns=['dfr_bin']).dfr_bin.to_numpy()
R['src']=0; P['src']=1; P['w']=0.0
keep=need+['dfr_bin','src']
X=pd.concat([R[keep],P[[c for c in keep if c!='xf_base']].assign(xf_base=np.nan)],ignore_index=True)
X['elb']=pd.cut(X.elapsed.clip(0,30),[-1,3,6,9,14,19,31],labels=False).astype(int)
X['clb']=np.where(X.period<=4,X.period.astype(int)*10+(X.clk//180).astype(int),50+(X.clk<=60).astype(int))
X['mb']=(X.margin.clip(-24,24)//6).astype(int)
zc=X.z.astype('category').cat.codes.astype(np.int64)
X['f_zs']=zc*10000+X.season; X['f_el']=zc*1000+X.elb*20+pd.factorize(X.start_type.astype(str))[0]; X['f_cl']=zc*100+X.clb
X['f_mb']=zc*100+X.mb; X['f_pen']=zc*10+X.def_pen.astype(int); X['f_dts']=X.deff.astype(np.int64)%100000*100000+X.season*10+X.is3
X['f_dfr']=zc*100+X.dfr_bin; X['pbb']=np.select([X.since_oreb<0,X.since_oreb<=1,X.since_oreb<=3,X.since_oreb<=6],[0,1,2,3],4); X['f_pb']=zc*10+X.pbb
fes=['f_zs','f_el','f_cl','f_mb','f_pen','f_dts','f_dfr','f_pb']
# cells a playoff row uses that have no RS weight
for f in fes:
    rw=X[X.src==0].groupby(f).w.sum(); miss=~X.loc[X.src==1,f].map(rw).fillna(0).gt(0)
    if miss.any(): print('  playoff rows in an RS-empty cell of',f,round(miss.mean(),5))
X['fouled']=X.fouled.astype(float)
XP=X[X.src==1][fes+['fouled']].reset_index(drop=True); XP['w0']=P.w0.to_numpy(); XP.to_parquet(f'{D}/xfoul_po_cells.parquet'); del XP   # for a playoff-only fit
X['xf_fit']=X.fouled-fe_resid(X,'fouled',fes,w='w',tol=1e-9)
rs=X[X.src==0]; print('RS refit vs stored: max abs diff',float(np.abs(rs.xf_fit-rs.xf_base).max()),' mean',float(np.abs(rs.xf_fit-rs.xf_base).mean()),flush=True)
P['xf_base']=X.loc[X.src==1,'xf_fit'].to_numpy(); del X,R,rs; gc.collect()
# referee crews
REF=pd.read_csv(f'{OUT}/ref_effects.csv').set_index('ref').effect_per_att
RF=pd.concat([pd.read_parquet(f) for f in glob.glob(f'{D}/out_po/refs_*.parquet')]); RF=RF[RF.refs.str.len()>0]
RF['crew']=[sum(REF.get(r,0.0) for r in s.split('|')) for s in RF.refs]; RF['known']=[np.mean([r in REF.index for r in s.split('|')]) for s in RF.refs]
P=P.merge(RF[['game_id','crew','known']],on='game_id',how='left'); P['crew']=P.crew.fillna(0)
print('playoff games with a named crew',round(P.drop_duplicates('game_id').known.notna().mean(),3),' refs known in RS',round(P.drop_duplicates('game_id').known.mean(),3),flush=True)
P['xf']=(P.xf_base+P.crew).clip(0.001,0.999)
P=P.merge(DV,on=['season','z'],how='left'); P['w']=P.w0
P['reaves_shot']=(P.fouled-P.xf)*P.dv*P.w; P['xf_w']=P.xf*P.w; P['f_w']=P.fouled*P.w
out=['game_id','season','off','deff','shooter','kind','z','w','loc_logged','is3','fouled','xf','xf_base','crew','dv','reaves_shot','f_w','xf_w','made','pts','period','clk','margin','def_pen','fouler','evnum']
P[out].to_parquet(f'{D}/xfoul_att_po.parquet')
L=P.groupby('season').agg(att=('w','sum'),fouled=('f_w','sum'),xf=('xf_w','sum')); L['ratio']=L.fouled/L.xf
print(L.round(3).to_string()); print('ALL playoffs fouls / RS-model xF',round(L.fouled.sum()/L.xf.sum(),4))
Z=P.groupby('z').agg(att=('w','sum'),fouled=('f_w','sum'),xf=('xf_w','sum')); Z['ratio']=Z.fouled/Z.xf; print(Z.round(3).to_string())
