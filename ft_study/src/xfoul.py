"""xFoul model and REAVES (proposed work items 2).
Unit: shooting attempt (FGA incl. and-ones, fouled miss). Target: fouled.
Problem: a fouled miss has no shot location (no FGA is logged). Solution, per season:
  For 2PT zones z (restricted area, paint non-RA, mid-range) a try is fouled w.p. phi_z; a fouled try goes in
  w.p. psi_z. Then fouled misses in z = and-ones in z * (1-psi_z)/psi_z. Assume psi_z = kappa * FG%_z(unfouled).
  For jump shots kappa is observed on 3s (4-point plays vs 3-FT trips, pooled over seasons) and applied to the
  mid-range; for the restricted area and paint, kappa is solved per season so the zone sum matches the season's
  observed 2PT fouled misses.
  Each player-season's 2PT fouled misses are spread over zones in proportion to his own and-one zone mix (shrunk
  toward the mix expected from his FGA locations with 10 pseudo and-ones) times (1-psi_z)/psi_z. 3PT fouled misses
  are spread over corner / above-the-break by his 3PA mix times the league foul-rate ratio.
xF: linear probability model with FEs: zone x season, zone x possession-age bin x start type (transition),
  zone x period x 3-min clock bin, zone x margin bin, zone x bonus state, defense team-season x 2/3,
  zone x decile of the on-court defenders' shooting-foul rate. Fractional zone rows are weighted.
  Referee effects (where the PBP names the crew) are added from a ridge fit of game-level residuals.
REAVES = sum over attempts of (fouled - xF) * (V_F - V_N): league points of a fouled vs unfouled attempt in that zone
  and season (league FT%), so it values foul drawing only, not the shooter's own FT or finishing skill."""
import pandas as pd, numpy as np, glob, gc, json
from fe import fe_resid
from collections import defaultdict
D='/home/claude/data'; OUT='/home/user/nba_data/ft_study/results'
import os
SUF='_imputed' if os.environ.get('FORCE_IMPUTE')=='1' else ''
if os.environ.get('RESUME')!='1':
    import pyarrow.parquet as pq
    cols=['game_id','season','evnum','shooter','kind','made','is3','dist','period','clk','elapsed','start_type','margin','def_tf','def_pen','deff','fouled','fouler','ftm','fta','pts']
    def dfr_rates(s):
        # defenders' shooting fouls committed per defensive possession (season), shrunk with 200 pseudo-possessions
        tb=pq.read_table(f'{D}/out/poss_{s}.parquet',columns=['def_lu']).column('def_lu').combine_chunks()
        v=tb.flatten().to_numpy(); u,c=np.unique(v,return_counts=True)
        Fo=pd.read_parquet(f'{D}/out/fouls_{s}.parquet',columns=['fouler','action'])
        sf=Fo[Fo.action.isin([2,29])].fouler.value_counts()
        k0=sf.sum()/max(c.sum(),1)
        return pd.Series((sf.reindex(u).fillna(0).to_numpy()+200*k0)/(c+200),index=u)
    parts=[]
    for f in sorted(glob.glob(f'{D}/out/att_*.parquet')):
        a=pd.read_parquet(f,columns=cols); s=int(a.season.iloc[0])
        lu=pq.read_table(f,columns=['def_lu']).column('def_lu').combine_chunks()
        off=lu.offsets.to_numpy(); vals=lu.flatten().to_numpy()
        rt=dfr_rates(s); rv=rt.reindex(vals).to_numpy()
        sums=np.add.reduceat(np.nan_to_num(rv),off[:-1]) if len(vals) else np.zeros(len(a))
        cnt=np.add.reduceat(~np.isnan(rv),off[:-1]).astype(float)
        ln=np.diff(off); sums=np.where(ln>0,sums,0); cnt=np.where(ln>0,cnt,0)
        a['dfr']=np.where(cnt>0,sums/np.maximum(cnt,1),np.nan).astype(np.float32)
        a=a[a.kind.isin(['fga','and1','fouled_miss'])&(a.shooter>0)]
        for c in ('made','is3','def_tf','def_pen','fouled','ftm','fta','pts'): a[c]=a[c].astype(np.int16)
        for c in ('clk','elapsed'): a[c]=a[c].astype(np.float32)
        a['kind']=a.kind.astype('category'); a['start_type']=a.start_type.astype('category')
        parts.append(a)
    A=pd.concat(parts,ignore_index=True); del parts; gc.collect()
    A['kind']=A.kind.astype(str); A['start_type']=A.start_type.astype(str)
    SD=pd.concat([pd.read_parquet(f,columns=['game_id','evnum','zone','sd_action']) for f in sorted(glob.glob(f'{D}/sd/sd_*.parquet'))],ignore_index=True)
    A=A.merge(SD,on=['game_id','evnum'],how='left'); del SD; gc.collect()
    zmap={'Restricted Area':'RA','In The Paint (Non-RA)':'PAINT','Mid-Range':'MID','Left Corner 3':'C3','Right Corner 3':'C3','Above the Break 3':'AB3','Backcourt':'AB3'}
    z=A.zone.astype(object).map(zmap)
    fb=np.select([A.is3==1,A.dist<0,A.dist<=4,A.dist<=9],['AB3','MID','RA','PAINT'],'MID')
    z=z.where(z.notna(),fb)
    z=np.where((A.is3==1)&~pd.Series(z).isin(['C3','AB3']),'AB3',z); z=np.where((A.is3==0)&pd.Series(z).isin(['C3','AB3']),'MID',z)
    A['z']=np.where(A.kind=='fouled_miss','?',z)
    A['jump']=A.sd_action.astype(str).str.contains('Jump|Fadeaway|Pullup|Step Back|Turnaround Hook',regex=True)
    print('attempts',len(A),'sd match share (FGA)',A.loc[A.kind!='fouled_miss','zone'].notna().mean().round(4),flush=True)
    Z2=['RA','PAINT','MID']; Z3=['C3','AB3']
    FORCE_IMPUTE=os.environ.get('FORCE_IMPUTE')=='1'   # validation: ignore logged foul locations (impute everywhere)
    # ---------- leak fix: a fouled-miss row is logged at its first FT, i.e. after the foul, so its def_tf / def_pen
    # already include that foul. Restore the state just before the foul from the foul record.
    Fo=pd.concat([pd.read_parquet(f,columns=['game_id','period','clk','fouler','tf_after','pen_before']) for f in sorted(glob.glob(f'{D}/out/fouls_*.parquet'))])
    Fo['c10']=(Fo.clk*10).round().astype(np.int64); Fo=Fo.drop_duplicates(['game_id','period','c10','fouler'])
    A['c10']=(A.clk.astype(np.float64)*10).round().astype(np.int64)
    ii=A.index[A.kind=='fouled_miss']
    mm=A.loc[ii,['game_id','period','c10','fouler']].merge(Fo[['game_id','period','c10','fouler','tf_after','pen_before']],on=['game_id','period','c10','fouler'],how='left')
    ok=mm.tf_after.notna().to_numpy()
    A.loc[ii[ok],'def_tf']=(mm.tf_after.to_numpy()[ok]-1).astype(np.int16); A.loc[ii[ok],'def_pen']=mm.pen_before.to_numpy()[ok].astype(np.int16)
    print('fouled misses with pre-foul state restored',round(ok.mean(),4),flush=True); del Fo,mm
    # ---------- logged foul locations (2016-17+): foul-spot zone of each fouled attempt
    LOC=pd.read_parquet(f'{D}/floc_att.parquet',columns=['game_id','att_evnum','kind','fz','sz','loc_ok'])
    a1=LOC[(LOC.kind=='and1')&LOC.loc_ok&LOC.sz.notna()]
    # the foul spot sits closer to the rim than the shot for some drives: P(shot zone | foul zone) from and-ones
    CONF=pd.crosstab(a1.fz,a1.sz).reindex(index=Z2+Z3,columns=Z2+Z3,fill_value=0).astype(float)
    CONF.round(0).to_csv(f'{OUT}/floc_confusion_and1.csv')
    lf=LOC[(LOC.kind=='fouled_miss')&LOC.loc_ok][['game_id','att_evnum','fz']].rename(columns={'att_evnum':'evnum'})
    A=A.merge(lf,on=['game_id','evnum'],how='left')
    if FORCE_IMPUTE: A['fz']=np.nan
    def conf_w(fz,is3):
        zs=Z3 if is3 else Z2
        r=CONF.loc[fz,zs].to_numpy()+1e-9 if fz in CONF.index else np.zeros(len(zs))
        return zs,(r/r.sum() if r.sum()>1e-6 else None)
    fmA=A[A.kind=='fouled_miss']
    print('fouled misses with a logged location',round(fmA.fz.notna().mean(),4),flush=True)
    # ---------- per season zone parameters
    par={}; rows=[]
    def logged_F(g):
        """expected fouled misses per shot zone from logged rows, scaled up for unlogged rows of the same 2/3 type"""
        Fz={zz:0.0 for zz in Z2+Z3}
        for is3 in (0,1):
            gg=g[(g.kind=='fouled_miss')&(g.is3==is3)]; lg=gg[gg.fz.notna()]
            acc={zz:0.0 for zz in Z2+Z3}
            for fz,c in lg.fz.value_counts().items():
                zs,w=conf_w(fz,is3)
                if w is None: continue
                for zz,ww in zip(zs,w): acc[zz]+=c*ww
            tot=sum(acc.values())
            if tot>0:
                for zz in acc: Fz[zz]+=acc[zz]*len(gg)/tot
        return Fz
    # kappa_z = psi_z / FG%(unfouled) pooled over logged seasons (QC'd); used to impute seasons without logs
    kap_num={zz:0.0 for zz in Z2+Z3}; kap_den={zz:0.0 for zz in Z2+Z3}; logged_seasons=[]
    for s,g in A.groupby('season'):
        if g[g.kind=='fouled_miss'].fz.notna().mean()<0.5: continue
        logged_seasons.append(s)
        fg=g[g.kind!='fouled_miss']; Aa=fg[fg.kind=='and1'].groupby('z').size(); gz=fg[fg.kind=='fga'].groupby('z').made.mean()
        Fz=logged_F(g)
        for zz in Z2+Z3:
            if Aa.get(zz,0)>0: kap_num[zz]+=Aa[zz]; kap_den[zz]+=(Aa[zz]+Fz[zz])*gz[zz]
    KAP={zz:(kap_num[zz]/kap_den[zz] if kap_den[zz]>0 else np.nan) for zz in Z2+Z3}
    if FORCE_IMPUTE:  # calibration must come from the logged data even when imputing: reuse the main run's kappas
        KAP=json.load(open(f'{D}/xfoul_kappa.json'))
    else:
        json.dump(KAP,open(f'{D}/xfoul_kappa.json','w'))
    print('kappa by zone (psi / unfouled FG%), pooled logged seasons',{k:round(v,3) for k,v in KAP.items()},flush=True)
    for s,g in A.groupby('season'):
        fg=g[g.kind!='fouled_miss']
        N=fg.groupby('z').size().reindex(Z2+Z3,fill_value=0); Aa=fg[fg.kind=='and1'].groupby('z').size().reindex(N.index,fill_value=0)
        un=fg[fg.kind=='fga']; gz=un.groupby('z').made.mean()
        F2=int(((g.kind=='fouled_miss')&(g.is3==0)).sum()); F3=int(((g.kind=='fouled_miss')&(g.is3==1)).sum())
        logged=s in logged_seasons and not FORCE_IMPUTE
        if logged:
            Fz=logged_F(g)
        else:
            raw={zz:Aa[zz]*(1-KAP[zz]*gz[zz])/(KAP[zz]*gz[zz]) for zz in Z2}
            sc=F2/sum(raw.values()); Fz={zz:raw[zz]*sc for zz in Z2}          # zone ratios from kappa, total observed
            A3=Aa[Z3].sum()
            for zz in Z3: Fz[zz]=F3*Aa[zz]/max(A3,1)
        psi={zz:Aa[zz]/(Aa[zz]+Fz[zz]) if Aa[zz]+Fz[zz]>0 else 0.2 for zz in Z2+Z3}
        phi={zz:(Aa[zz]+Fz[zz])/(N[zz]+Fz[zz]) for zz in Z2+Z3}
        par[s]=dict(psi=psi,phi=phi,logged=int(logged),g=gz.to_dict(),arate={zz:Aa[zz]/max(N[zz],1) for zz in Z2+Z3})
        rows.append(dict(season=s,logged=int(logged),F2=F2,F3=F3,**{f'phi_{zz}':phi[zz] for zz in Z2+Z3},**{f'psi_{zz}':psi[zz] for zz in Z2+Z3},**{f'kappa_{zz}':psi[zz]/gz[zz] for zz in Z2}))
    ZP=pd.DataFrame(rows); ZP.round(4).to_csv(f'{OUT}/xfoul_zone_params{"_imputed" if FORCE_IMPUTE else ""}.csv',index=False); print(ZP.round(3).to_string(),flush=True)
    # ---------- zone weights for each fouled miss: logged (confusion-corrected) or imputed from the player's mix
    fm=A[A.kind=='fouled_miss']
    fg=A[A.kind!='fouled_miss']
    n_pz=fg.groupby(['shooter','season','z']).size().unstack(fill_value=0)
    a_pz=fg[fg.kind=='and1'].groupby(['shooter','season','z']).size().unstack(fill_value=0).reindex(n_pz.index,fill_value=0)
    for zz in Z2+Z3:
        if zz not in n_pz: n_pz[zz]=0
        if zz not in a_pz: a_pz[zz]=0
    W={}
    m=10.0
    for (p,s) in n_pz.index:
        P_=par[s]; n=n_pz.loc[(p,s)]; a=a_pz.loc[(p,s)]
        e2=np.array([n[zz]*P_['arate'][zz] for zz in Z2]); a2=np.array([a[zz] for zz in Z2],float)
        sh=(a2+m*e2/max(e2.sum(),1e-9))/(a2.sum()+m) if e2.sum()>0 else np.array([1/3]*3)
        w2=sh*np.array([(1-P_['psi'][zz])/P_['psi'][zz] for zz in Z2]); w2=w2/w2.sum()
        n3=np.array([n[zz]*P_['phi'][zz] for zz in Z3],float)
        w3=n3/n3.sum() if n3.sum()>0 else np.array([.2,.8])
        W[(p,s)]=(w2,w3)
    ex=[]
    for r in fm[['shooter','season','is3','fz']].itertuples():
        w=None
        if isinstance(r.fz,str):
            zs,w=conf_w(r.fz,r.is3)
        if w is None:
            key=(r.shooter,r.season)
            if key in W: w2,w3=W[key]
            else:
                P_=par[r.season]; w2=np.array([(1-P_['psi'][zz])/P_['psi'][zz]*P_['arate'][zz] for zz in Z2]); w2=w2/w2.sum(); w3=np.array([.2,.8])
            zs,w=(Z3,w3) if r.is3 else (Z2,w2); src=0
        else: src=1
        for zz,ww in zip(zs,w):
            if ww>1e-6: ex.append((r.Index,zz,ww,src))
    EX=pd.DataFrame(ex,columns=['idx','z','w','loc_logged'])
    FMX=fm.drop(columns=['z']).loc[EX.idx].reset_index(drop=True); FMX['z']=EX.z.values; FMX['w']=EX.w.values; FMX['loc_logged']=EX.loc_logged.values
    FG=fg.copy(); FG['w']=1.0; FG['loc_logged']=1
    X=pd.concat([FG,FMX],ignore_index=True); del FG,FMX,fm,fg; gc.collect()
    X['att_id']=np.r_[np.arange((A.kind!='fouled_miss').sum()), (A.kind!='fouled_miss').sum()+pd.factorize(EX.idx)[0]]
    X['dfr_bin']=X.groupby('season').dfr.transform(lambda v: pd.qcut(v.rank(method='first'),10,labels=False)).fillna(-1).astype(int)
    # ---------- FE cells
    X['elb']=pd.cut(X.elapsed.clip(0,30),[-1,3,6,9,14,19,31],labels=False).astype(int)
    X['clb']=np.where(X.period<=4,X.period.astype(int)*10+(X.clk//180).astype(int),50+(X.clk<=60).astype(int))
    X['mb']=(X.margin.clip(-24,24)//6).astype(int)
    zc=X.z.astype('category').cat.codes.astype(np.int64)
    X['f_zs']=zc*10000+X.season
    X['f_el']=zc*1000+X.elb*20+pd.factorize(X.start_type)[0]
    X['f_cl']=zc*100+X.clb
    X['f_mb']=zc*100+X.mb
    X['f_pen']=zc*10+X.def_pen
    X['f_dts']=X.deff.astype(np.int64)%100000*100000+X.season*10+X.is3
    X['f_dfr']=zc*100+X.dfr_bin
    fes=['f_zs','f_el','f_cl','f_mb','f_pen','f_dts','f_dfr']
    X['fouled']=X.fouled.astype(float)
    X['xf_base']=X.fouled-fe_resid(X,'fouled',fes,w='w',tol=1e-9)
    print('xf fit done', flush=True)
    X.to_parquet(f'{D}/xfoul_stage1{SUF}.parquet'); json.dump({str(k):{kk:(vv if not isinstance(vv,dict) else {q:float(b) for q,b in vv.items()}) for kk,vv in v.items()} for k,v in par.items()},open(f'{D}/xfoul_par{SUF}.json','w'),default=float)
else:
    X=pd.read_parquet(f'{D}/xfoul_stage1{SUF}.parquet'); par={int(k):v for k,v in json.load(open(f'{D}/xfoul_par{SUF}.json')).items()}

# ---------- referee effects from game-level residuals (ridge), where crews are named
R=pd.concat([pd.read_parquet(f) for f in glob.glob(f'{D}/out/refs_*.parquet')])
R=R[R.refs.str.len()>0]
X['res']=(X.fouled-X.xf_base)*X.w
G=X.groupby('game_id').agg(res=('res','sum'),n=('w','sum')).join(R.set_index('game_id'),how='inner')
refl=sorted({r for s in G.refs for r in s.split('|')}); ri={r:i for i,r in enumerate(refl)}
import scipy.sparse as sp, scipy.sparse.linalg as sla
rr,cc,vv=[],[],[]
for i,s in enumerate(G.refs):
    for r in s.split('|'): rr.append(i); cc.append(ri[r]); vv.append(1.0)
M=sp.csr_matrix((vv,(rr,cc)),shape=(len(G),len(refl)))
sw=np.sqrt(G.n.to_numpy()); yv=(G.res/G.n).to_numpy()
lam=30.0  # ridge in units of games
sol=sla.lsqr(sp.diags(sw)@M,sw*yv,damp=np.sqrt(lam*G.n.mean()))[0]
REF=pd.DataFrame({'ref':refl,'effect_per_att':sol,'games':np.asarray(M.sum(0)).ravel()}).sort_values('effect_per_att')
REF.to_csv(f'{OUT}/ref_effects{SUF}.csv',index=False)
gref=pd.Series(np.asarray(M@sol).ravel(),index=G.index)
X['xf']=(X.xf_base+X.game_id.map(gref).fillna(0)).clip(0.001,0.999)
print('ref games',len(G),'refs',len(refl),'ref effect sd (per attempt)',round(sol.std(),4),'crew sum sd',round(gref.std(),4),flush=True)
# ---------- value of a foul vs no foul in zone-season
_ft=pd.concat([pd.read_parquet(f,columns=['season','kind','fta','ftm']) for f in sorted(glob.glob(f'{D}/out/att_*.parquet'))]); _ft=_ft[_ft.kind.isin(['and1','fouled_miss'])]
lgft=_ft.groupby('season').ftm.sum()/_ft.groupby('season').fta.sum()
vn=X[(X.kind=='fga')].groupby(['season','z']).pts.mean().rename('VN')
X=X.join(vn,on=['season','z'])
X['pv']=np.where(X.is3==1,3,2)
X['psi']=[par[s]['psi'][zz] for s,zz in zip(X.season,X.z)]
X['ftp']=X.season.map(lgft)
X['VF']=X.psi*(X.pv+X.ftp)+(1-X.psi)*X.pv*X.ftp
X['dv']=X.VF-X.VN
X['reaves_shot']=(X.fouled-X.xf)*X.dv*X.w
X['xf_w']=X.xf*X.w; X['f_w']=X.fouled*X.w
X['jump_fga']=(X.kind!='fouled_miss')&X.jump
keep=['game_id','season','shooter','kind','z','w','loc_logged','is3','fouled','xf','xf_base','dv','reaves_shot','f_w','xf_w','jump_fga','made','pts','period','clk','def_tf','def_pen','margin','att_id','fouler']
X[keep].to_parquet(f'{D}/xfoul_att{SUF}.parquet')
# calibration
cal=X.groupby(pd.cut(X.xf,[0,.02,.05,.08,.12,.2,.3,1])).apply(lambda g: pd.Series({'n':g.w.sum(),'xf':(g.xf*g.w).sum()/g.w.sum(),'obs':(g.fouled*g.w).sum()/g.w.sum()}))
print(cal.round(4).to_string())
lv=X.groupby('season').apply(lambda g: pd.Series({'att':g.w.sum(),'fouled':g.f_w.sum(),'xf':g.xf_w.sum(),'dv_mean':(g.dv*g.w).sum()/g.w.sum()}))
lv.round(4).to_csv(f'{OUT}/xfoul_league{SUF}.csv'); print(lv.round(3).to_string())
json.dump({str(k):{kk:(vv if not isinstance(vv,dict) else {a:float(b) for a,b in vv.items()}) for kk,vv in v.items()} for k,v in par.items()},open(f'{D}/xfoul_par{SUF}.json','w'),default=float)
