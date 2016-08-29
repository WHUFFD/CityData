##前置需求
library(data.table)
library(dplyr)
library(fpc)

##数据拆分
predbscansws<-function(data){
    n=nrow(data)
    n=ifelse(n%%10000==0,n%/%10000,n%/%10000+1)
    rs=list()
    for(i in 1:n){
        if(i==n){
            rs[[i]]=data[((i-1)*10000+1):nrow(data)]
        } else{
            rs[[i]]=data[((i-1)*10000+1):(i*10000)]
        }
    }
    return(rs)
}

##局部聚类
#子函数：距离分布矩阵
distcomb <- function(x,data){
    data=t(data)
    temp=apply(x, 1, function(x){
        sqrt(colSums((data-x)^2))
    })
    if (is.null(dim(temp)))
        return(as.matrix(temp, nrow(x), ncol(data)))
    else
        return(t(temp))
}
#子函数
dbft<-function(exp,dat){
    ds=dbscan(dat,exp,30)
    a=(length(unique(ds$cluster))-1)
    t=length(which(ds$isseed==FALSE))
    return(c(a,t))
}
#子函数
dbfp<-function(dists,eps,dat,pts){
    dd=copy(dat)
    tmp=which(dists<=eps)
    k=pts[tmp,]
    setkey(dd,longitude,latitude)
    setkey(k,longitude,latitude)
    dd=dd[k,]
    return(count(dd)$n)
}
#子函数：寻找最佳聚类参数
lfke<-function(wdatbak){
    x=unique(wdatbak[,list(longitude,latitude)])
    disa=data.table(as.matrix(dist(x)))
    disb=data.table(apply(disa,1,sort))
    epstab=data.table(apply(disb,1,mean))
    setnames(epstab,"V1","eps")
    epstab=epstab[1:40]
    k=apply(epstab,1,dbft,wdatbak[,list(longitude,latitude)])
    epstab[,c("areas","trash"):=list(k[1,],k[2,])]
    epstab[,c("da","dt"):=list((lead(areas,1L)-lag(areas,1L))/2,(lead(trash,1L)-lag(trash,1L))/2)]
    p=epstab[!is.na(da),]
    tp=copy(p)
    if(length(unique(p$areas))>2 & length(unique(p$trash))>2){
        tp=tp[!(areas==max(p$areas)),]
        tp=tp[!(areas==min(p$areas)),]
        tp=tp[!(trash==min(p$trash)),]
        tp=tp[!(trash==max(p$trash)),]
    }
    t1=sort(unique(abs(tp$da)))
    t2=sort(unique(abs(tp$dt)))
    l=min(length(t1),length(t2))
    for(i in 1:l){
        tp1=tp[abs(da)<=t1[i],]
        if(!(length(tp1$eps)==0)) break
    }
    for(i in 1:l){
        tp2=tp[abs(dt)<=t2[i],]
        if(!(length(tp2$eps)==0)) break
    }
    tp1=tp1[abs(dt)==min(abs(dt)),]
    tp2=tp2[abs(da)==min(abs(da)),]
    tp=rbind(tp1,tp2)
    r=which.min(tp$eps)
    eps=tp[r,]$eps
    f=apply(disa,1,dbfp,eps,wdatbak,x)
    minpts=sum(f)/length(wdatbak$longitude)
    return(list(eps,minpts))
}
#子函数：对单个节点进行聚类
dbscans<-function(data){
    eam=lfke(data)
    gc()
    ds=dbscan(data,eam[[1]],eam[[2]])
    cl=max(ds$cluster)
    rlt=list(data=data,ds=ds)
    rs=data.table(cluster=c(1:cl))
    rs[,c("longitude","latitude","er"):=list(as.numeric(NA),as.numeric(NA),as.numeric(NA))]
    for(j in 1:cl){
        tmp=data[ds$cluster==j]
        mlng=(max(tmp$longitude)+min(tmp$longitude))/2
        mlat=(max(tmp$latitude)+min(tmp$latitude))/2
        n=which.min(distcomb(data.table(longitude=mlng,latitude=mlat),tmp))
        mlng=tmp[n]$longitude
        mlat=tmp[n]$latitude
        r=max(distcomb(data.table(longitude=mlng,latitude=mlat),tmp))
        rs[cluster==j,c("longitude","latitude","er"):=list(mlng,mlat,r)]
    }
    rlt$scor=rs
    return(rlt)
}
#主函数
dbscansws<-function(data){
    t=lapply(data,dbscans)
    gc()
    return(t)
}

##全局数据合并
#子函数：计算某代表点的中心点
ffso<-function(p,scor){
    p=as.data.table(t(p))
    tmpdist1=as.vector(distcomb(p[,list(longitude,latitude)],scor[,list(longitude,latitude)]))
    tmpdist2=scor$er+p$er
    np=scor[tmpdist1<=tmpdist2 & !(tmpdist1==0) & !(tmpdist2==0)]
    if(nrow(np)==0){
        lngc=p$longitude
        latc=p$latitude
        erc=p$er
    } else{
        lngc=mean(np$longitude)
        latc=mean(np$latitude)
        erc=mean(np$er)
    }
    return(c(lngc,latc,erc))
}
#子函数：计算某节点的所有中心点
ffs<-function(scort){
    scor=scort$scor
    t=apply(scor,1,ffso,scor)
    scor[,c("lngc","latc","erc"):=list(t[1,],t[2,],t[3,])]
    return(scor)
}
#主函数
dbscantws<-function(data){
    n=length(data)
    dat=copy(data)
    scorc=lapply(data,ffs)
    for(i in 1:n){
        dat[[i]]$scor=scorc[[i]]
        dat[[i]]$scor[,cnn:=as.numeric(0)]
    }
    gc()
    cn=0
    for(i in 1:n){
        n1=nrow(dat[[i]]$scor)
        for(j in 1:n){
            if(j==i) next
            n2=nrow(dat[[j]]$scor)
            for(m in 1:n1){
                for(k in 1:n2){
                    d=as.vector(distcomb(dat[[i]]$scor[m][,list(longitude,latitude)],dat[[j]]$scor[k][,list(longitude,latitude)]))
                    dc=as.vector(distcomb(dat[[i]]$scor[m][,list(lngc,latc)],dat[[j]]$scor[k][,list(lngc,latc)]))
                    r=(dat[[i]]$scor[m]$er+dat[[j]]$scor[k]$er)-((dat[[i]]$scor[m]$er+dat[[j]]$scor[k]$er)-(dat[[i]]$ds$eps+dat[[j]]$ds$eps))/2
                    rc=(dat[[i]]$scor[m]$erc+dat[[j]]$scor[k]$erc)-((dat[[i]]$scor[m]$erc+dat[[j]]$scor[k]$erc)-(dat[[i]]$ds$eps+dat[[j]]$ds$eps))/2
                    if(d<=r & dc<=rc){
                        if(dat[[i]]$scor[m]$cnn==0 & dat[[j]]$scor[k]$cnn==0){
                            cn=cn+1
                            dat[[i]]$scor[m]$cnn=cn
                            dat[[j]]$scor[k]$cnn=cn
                        } else
                        if(!(dat[[i]]$scor[m]$cnn==0) & dat[[j]]$scor[k]$cnn==0){
                            dat[[j]]$scor[k]$cnn=dat[[i]]$scor[m]$cnn
                        } else
                        if(dat[[i]]$scor[m]$cnn==0 & !(dat[[j]]$scor[k]$cnn==0)){
                            dat[[i]]$scor[m]$cnn=dat[[j]]$scor[k]$cnn
                        }
                    }
                }
            }
        }
    }
    gc()
    for(i in 1:n){
        n1=nrow(dat[[i]]$scor)
        for(j in 1:n1){
            if(dat[[i]]$scor[j]$cnn==0){
                cn=cn+1
                dat[[i]]$scor[j]$cnn=cn
            }
        }
    }
    gc()
    noises=data.table(longitude=as.numeric(0),latitude=as.numeric(0),cnn=as.numeric(0))
    allsets=data.table(longitude=as.numeric(0),latitude=as.numeric(0),er=as.numeric(0),eps=as.numeric(0),cnn=as.numeric(0))
    for(i in 1:n){
        tmp=dat[[i]]$data[dat[[i]]$ds$cluster==0]
        tmp[,cnn:=as.numeric(0)]
        noises=rbind(noises,tmp)
        tmp=dat[[i]]$scor[,list(longitude,latitude,er,cnn)]
        tmp[,eps:=dat[[i]]$ds$eps]
        allsets=rbind(allsets,tmp)
    }
    gc()
    noises=noises[!(longitude==0) & !(latitude==0),]
    allsets=allsets[!(longitude==0) & !(latitude==0) & !(er==0)]
    nn=nrow(noises)
    for(i in 1:nn){
        tmpdist=as.vector(distcomb(noises[i,list(longitude,latitude)],allsets[,list(longitude,latitude)]))
        minone=which.min(tmpdist)
        if((tmpdist[minone]-allsets[minone,]$er)<=allsets[minone,]$eps){
            noises[i,cnn:=allsets[minone,]$cnn]
        }
    }
    gc()
    nmnoises=noises[!(cnn==0),]
    noises=noises[cnn==0,]
    eam=lfke(noises[,list(longitude,latitude)])
    mpttab=numeric(0)
    epstab=numeric(0)
    for(i in 1:n){
        mpttab=union(mpttab,dat[[i]]$ds$MinPts)
        epstab=union(epstab,dat[[i]]$ds$eps)
    }
    if(!(eam[[1]]>max(epstab) | eam[[2]]<min(mpttab))){
        ds=dbscan(noises[,list(longitude,latitude)],eam[[1]],eam[[2]])
        noises[,cl:=ds$cluster]
        setkey(noises,cl)
        noises[!(cl==0),j:=ifelse((cl-shift(cl,1L))==0,0,1)]
        noises[cl==1 & is.na(j),j:=1]
        noises[!(cl==0),sj:=(cumsum(j)+cn)]
        noises[!(cl==0),cnn:=sj+cn]
        noises[,c("cl","j","sj"):=list(NULL,NULL,NULL)]
    }
    noises=unique(rbind(nmnoises,noises))
    gc()
    for(i in 1:n){
        tmp=data.table(cl=dat[[i]]$scor$cluster,cnn=dat[[i]]$scor$cnn)
        setkey(tmp,cl)
        tmpcnn=tmp$cnn
        tmpcnn=c(0,tmpcnn)
        dat[[i]]$ds$cluster=tmpcnn[(dat[[i]]$ds$cluster+1)]
        noiseset=which(dat[[i]]$ds$cluster==0)
        for(j in noiseset){
            snoise=noises[longitude==dat[[i]]$data[j,]$longitude & latitude==dat[[i]]$data[j,]$latitude,]
            snoise=snoise[1]
            dat[[i]]$ds$cluster[j]=snoise[1]$cnn
        }
    }
    gc()
    return(dat)
}

##数据整理
aftdbscantws<-function(data,wdatbak){
    n=length(data)
    V10=numeric(0)
    for(i in 1:n){
        tmp=data[[i]]$ds$cluster
        V10=c(V10,tmp)
    }
    gc()
    dat=cbind(wdatbak,data.table(V10))
    dat=dat[!(V10==0),]
    return(dat)
}

##DBDC算法
dbdc<-function(wdatbak){
    dat=predbscansws(wdatbak[,list(longitude,latitude)])
    rlt=dbscansws(dat)
    rlt=dbscantws(rlt)
    rlt=aftdbscantws(rlt,wdatbak)
    return(rlt)
}
