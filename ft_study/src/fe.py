import numpy as np, pandas as pd
def demean(M, codes, tol=1e-8, maxit=500, w=None):
    """Alternating projections: remove (weighted) group means for each FE in turn until converged."""
    M=np.array(M,dtype=np.float64,copy=True,order="C")
    if w is None: w=np.ones(M.shape[0])
    cnts=[np.bincount(c,weights=w) for c in codes]
    for it in range(maxit):
        mx=0
        for c,n in zip(codes,cnts):
            for j in range(M.shape[1]):
                gm=np.bincount(c,weights=w*M[:,j],minlength=len(n))/np.maximum(n,1e-12)
                M[:,j]-=gm[c]
                mx=max(mx,np.abs(gm).max())
        if mx<tol: break
    return M
def feols(df, ys, xs, fes, cluster, w=None):
    """Multi-way FE OLS/WLS with cluster-robust SEs. w: column name of analytic weights (optional)."""
    codes=[pd.factorize(df[f])[0] for f in fes]
    wv=None if w is None else df[w].to_numpy(np.float64)
    Y=df[ys].to_numpy(np.float64); X=df[xs].to_numpy(np.float64)
    D=demean(np.hstack([Y,X]),codes,w=wv)
    Yd,Xd=D[:,:len(ys)],D[:,len(ys):]
    sw=np.ones(len(df)) if wv is None else np.sqrt(wv)
    Xw=Xd*sw[:,None]; Yw=Yd*sw[:,None]
    XtX=Xw.T@Xw; inv=np.linalg.pinv(XtX)
    B=inv@Xw.T@Yw
    cl=pd.factorize(df[cluster])[0]; G=cl.max()+1
    out={}
    for k,y in enumerate(ys):
        u=(Yw[:,k]-Xw@B[:,k])
        S=np.zeros((G,Xd.shape[1]))
        for j in range(Xd.shape[1]): S[:,j]=np.bincount(cl,weights=Xw[:,j]*u,minlength=G)
        V=inv@(S.T@S)@inv*G/(G-1)
        out[y]=(B[:,k],np.sqrt(np.diag(V)))
    return out
def fe_resid(df, y, fes, w=None, tol=1e-8):
    """Return y with the FEs projected out (for fitted values: y - resid)."""
    codes=[pd.factorize(df[f])[0] for f in fes]
    wv=None if w is None else df[w].to_numpy(np.float64)
    return demean(df[[y]].to_numpy(np.float64),codes,tol=tol,w=wv)[:,0]
