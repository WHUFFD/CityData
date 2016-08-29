##前置需求
library(data.table)
library(dplyr)
library(fpc)

##子函数：建立空间格网
crpg<-function(x,minlng,minlat,step){
    lng1=x[[1]]
    lat1=x[[2]]
    dlng=lng1-minlng
    dlat=lat1-minlat
    rown=ifelse(dlng%%step==0,dlng%/%step,dlng%/%step+1)
    coln=ifelse(dlat%%step==0,dlat%/%step,dlat%/%step+1)
    return(c(rown,coln))
}

##子函数：建立空间距离矩阵
distcomb <- function(x,data){
    x=x[,list(longitude,latitude)]
    data=data[,list(longitude,latitude)]
    data <- t(data)
    temp <- apply(x, 1, function(x){
      sqrt(colSums((data-x)^2))
    })
    if (is.null(dim(temp)))
      matrix(temp, nrow(x), ncol(data))
    else
      t(temp)
}

##主函数：基于空间格网的DBSCAN
dbscanbg<-function(data,eps,MinPts = 5){
    dat=copy(data)
    minlng=min(dat$longitude)
    minlat=min(dat$latitude)
    if(eps==0) step=0.03
    else step=3*eps
    t=apply(dat,1,crpg,minlng,minlat,step)
    dat[,c("rown","coln"):=list(t[1,],t[2,])]
    n=nrow(dat)
    classn=integer(n)
    cv=integer(n)
    isseed=logical(n)
    cn=integer(1)
    arow=dat$rown
    acol=dat$coln
    for(i in 1:n){
        rownn=dat[i]$rown
        colnn=dat[i]$coln
        unclass=(1:n)[arow>=(rownn-1) & arow<=(rownn+1) & acol>=(colnn-1) & acol<=(colnn+1) & cv<1]
        if(!length(unclass)) next
        if (cv[i]==0){
            reachables=unclass[as.vector(distcomb(dat[i,, drop=FALSE],dat[unclass,, drop=FALSE]))<=eps]
            if (length(reachables)+classn[i]<MinPts){
                cv[i]=(-1)
            }
            else{
                cn=cn+1
                cv[i]=cn
                isseed[i]=TRUE
                reachables=setdiff(reachables, i)
                unclass=setdiff(unclass, i)
                classn[reachables]=classn[reachables]+1
                while (length(reachables)){
                    cv[reachables]=cn
                    ap=reachables
                    reachables=integer()
                    unclass=integer()
                    for(m in ap){
                        rownn=dat[m]$rown
                        colnn=dat[m]$coln
                        tmp=(1:n)[arow>=(rownn-1) & arow<=(rownn+1) & acol>=(colnn-1) & acol<=(colnn+1) & cv<1]
                        unclass=union(unclass,tmp)
                    }
                    unclass=unique(sort(unclass))
                    if(!length(unclass)) {
                        cv[reachables[cv[reachables]<0]]<-cn
                        break
                    }
                    tempdist=distcomb(dat[ap, , drop=FALSE], dat[unclass, , drop=FALSE])
                    frozen.unclass=unclass
                    for (i2 in seq(along=ap)){
                        j=ap[i2]
                        jreachables=unclass[tempdist[i2,match(unclass, frozen.unclass)]<=eps]
                        if (length(jreachables)+classn[j]>=MinPts){
                            isseed[j]=TRUE
                            cv[jreachables[cv[jreachables]<0]]<-cn
                            reachables=union(reachables, jreachables[cv[jreachables]==0])
                        }
                        classn[jreachables]=classn[jreachables]+1
                        unclass=setdiff(unclass, j)
                    }
                }
            }
        }
    }
    if (any(cv==(-1))){
        cv[cv==(-1)]<-0
    }
    out=list(cluster=cv,eps=eps,MinPts=MinPts)
    if (cn>0){
        out$isseed <- isseed
    }
    class(out) <- "dbscan"
    return(out)
}
