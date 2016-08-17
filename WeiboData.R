###前置需求
library(RPostgreSQL)
library(data.table)
library(dplyr)
library(rgeos)
library(rgdal)
library(fpc)

###函数定义开始

##筛选停留点
#子函数：月份转为天数
mtd<-function(mo){
    if(mo==2){
        return(31)
    }
    else if(mo==3){
        return(31+28)
    }
    else if(mo==4){
        return(31+28+31)
    }
    else if(mo==5){
        return(31+28+31+30)
    }
    else if(mo==6){
        return(31+28+31+30+31)
    }
    else if(mo==7){
        return(31+28+31+30+31+30)
    }
    else if(mo==8){
        return(31+28+31+30+31+30+31)
    }
    else if(mo==9){
        return(31+28+31+30+31+30+31+31)
    }
    else if(mo==10){
        return(31+28+31+30+31+30+31+31+30)
    }
    else if(mo==11){
        return(31+28+31+30+31+30+31+31+30+31)
    }
    else if(mo==12){
        return(31+28+31+30+31+30+31+31+30+31+30)
    }
    else if(mo==1){
        return(0)
    }
}
#子函数：日期转化为秒数
tts<-function(yy,mo,dd,hh,mm,ss){
    mo=mtd(mo)
    rlt=ifelse((yy-2014)==0,mo+dd,365+mo+dd)*24*3600+hh*3600+mm*60+ss
    return(rlt)
}
#主函数
fsp<-function(oridat){
    wdat=copy(oridat)
    wdat[,c("yy","mo","dd","hh","mm","ss"):=list(as.numeric(substr(V2,1,4)),as.numeric(substr(V2,6,7)),as.numeric(substr(V2,9,10)),as.numeric(substr(V2,12,13)),as.numeric(substr(V2,15,16)),as.numeric(substr(V2,18,19)))]
    t=mapply(tts,wdat$yy,wdat$mo,wdat$dd,wdat$hh,wdat$mm,wdat$ss)
    wdat[,time:=t]
    wdat[,c("V3","V4"):=list(as.numeric(V3),as.numeric(V4))]
    wdat <- data.frame(wdat, x=wdat$V3, y=wdat$V4)
    coordinates(wdat) <- c("x", "y")
    proj4string(wdat) <- CRS("+init=epsg:4326 +proj=longlat +ellps=WGS84 + datum=WGS84")
    wdat <- spTransform(wdat, CRS("+init=epsg:2414 +datum=WGS84"))
    wdat <- data.table(coordinates(wdat), wdat@data)
    wdat[,c:=length(time),by=V1]
    nouse=wdat[c==1 | c==2,]
    nouse[,c("c","type"):=list(NULL,as.numeric(1))]
    rlt=cbind(nouse)
    wdat=wdat[!(c==1 | c==2),]
    setkey(wdat,V1,time)
    wdat[,dtime:=(time-shift(time,1L)),by=V1]
    wdat[,dis:=sqrt((x-shift(x,1L))*(x-shift(x,1L)) + ((y-shift(y,1L))*(y-shift(y,1L))))]
    wdat[,avgS:=(dis/dtime)]
    wdat[is.na(dtime),c("dtime","dis","avgS"):=list(0,0,0)]
    wdat[,type:=ifelse(avgS>0.6,2,1)]
    wdat[,j:=ifelse((type-shift(type,1L))==0,0,1),by=V1]
    wdat[is.na(j),j:=1]
    wdat[,tt:=cumsum(j),by=V1]
    wdat[,c("adtime","adis"):=list(sum(dtime),sum(dis)),by=.(V1,tt)]
    wdat[type==1 & adtime<=30 & !(adtime==0),type:=2]
    wdat[type==2 & adis<=200 & !(adis==0),type:=1]
    wdat[,c("c","j","tt","adtime","adis","dtime","dis","avgS"):=list(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)]
    rlt=rbind(rlt,wdat)
    return(rlt)
}

##根据数据计算最佳dbscan聚类参数
#子函数：距离计算函数
dista<-function(x1,y1,x2,y2){
    dx=abs(x1-x2)
    dy=abs(y1-y2)
    dis=sqrt(dx*dx+dy*dy)
    return(dis)
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
#主函数
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

##dbscan聚类
wdbs<-function(rs,eps,minpts){
    x=rs[,list(longitude,latitude)]
    ds=dbscan(x,eps,minpts)
    V10=ds$cluster
    fc=cbind(rs,V10)
    fc=fc[!(V10==0),]
    fc=na.omit(fc)
    return(fc)
}

##HITS算法，根据签到点计算hub和authority
#子函数
coea<-function(hub,tabn,col){
     auth=t(tabn[row == hub[[1]],])[2:(length(col)+1)]
     auth=as.numeric(auth)
     auth=auth*as.numeric(hub[[2]])
     return(auth)
}
#子函数
coeh<-function(authority,tabn){
    m=as.numeric(authority[[1]])
    hu=as.numeric(tabn[[m+1]])
    hu=hu*as.numeric(authority[[2]])
    return(hu)
}
#子函数
cotn<-function(i,row,dat){
    tmp=dat[V10==i,]
    setkey(tmp,userid)
    tmp=tmp[row,]
    tmp[is.na(count),count:=0]
    return(tmp$count)
}
#主函数
hits<-function(wdatbak){
    dat=copy(wdatbak)
    setkey(dat,V10,userid)
    dat[,con:=length(longitude),by=.(V10,userid)]
    dat=data.table(userid=dat$userid,V10=dat$V10,count=dat$con)
    dat=unique(dat,fromFirst=TRUE)
    col=unique(dat$V10)
    row=unique(dat$userid)
    tabn=cbind(data.table(row),data.table(apply(data.table(col),1,cotn,row,dat)))
    for(i in col){
        setnames(tabn,paste("V",i,sep=""),paste(i))
    }
    hub=data.table(row)
    authority=data.table(col)
    hub[,hub:=1]
    authority[,authority:=1]
    a_old=authority$col
    h_old=hub$row
    jugg=0
    while(TRUE){
        f=apply(hub,1,coea,tabn,col)
        f=apply(f,1,sum)
        authority[,authority:=f]
        down_a=authority[,authority]
        down_a=down_a*down_a
        down_a=sqrt(sum(down_a))
        authority[,authority:=(authority/down_a)]
        f=apply(authority,1,coeh,tabn)
        f=apply(f,1,sum)
        hub[,hub:=f]
        down_h=hub[,hub]
        down_h=down_h*down_h
        down_h=sqrt(sum(down_h))
        hub[,hub:=(hub/down_h)]
        tmp=copy(authority)
        setkey(tmp,authority)
        a=tmp$col
        tmp=copy(hub)
        setkey(tmp,hub)
        h=tmp$row
        if(all(a==a_old) & all(h==h_old)){
            jugg=jugg+1
        }
        else{
            jugg=0
        }
        if(jugg==10){
            break
        }
        a_old=a
        h_old=h
    }
    return(list(authority,hub))
}

##求得初始区域间网络
#子函数
coet<-function(i,col,dat){
    co=as.character(col)
    tmp=dat[end==i,]
    tmp[,start:=as.character(start)]
    setkey(tmp,start)
    tmp=tmp[co,]
    tmp[is.na(con),con:=0]
    return(tmp$con)
}
#主函数
cot<-function(wdatbak){
    dat=copy(wdatbak)
    setkey(dat,V10,userid)
    col=unique(dat$V10)
    row=unique(dat$userid)
    dat=data.table(dat$userid,dat$checkin_time,dat$V10)
    setkey(dat,V1,V2)
    dat[,con:=length(V2),by=V1]
    dat=dat[!(con==1),]
    dat[,c("start","end"):=list(shift(V3,1L),V3),by=V1]
    dat=dat[!is.na(start) & !(start==end),]
    dat[,con:=length(V2),by=.(start,end)]
    dat=data.table(start=dat$start,end=dat$end,con=dat$con)
    dat=unique(dat)
    ori_net=cbind(data.table(col),data.table(apply(data.table(col),1,coet,col,dat)))
    for(i in col){
        setnames(ori_net,paste("V",i,sep=""),paste(i))
    }
    return(ori_net)
}

##求得最终区域间网络
#子函数
cone<-function(i,n,orinet,authority,hub){
    if(orinet[col==n,][[i+1]]==0) return(0)
    outn=sum(orinet[col==n,])-n
    inn=sum(orinet[[i+1]])
    a=orinet[col==n,][[i+1]]*((orinet[col==n,][[i+1]]/outn)*authority[col==n,authority]+(orinet[col==n,][[i+1]]/inn)*authority[col==i,authority])
    return(a)
}
#子函数
conea<-function(i,col,orinet,authority,hub){
    a=apply(data.table(col),1,cone,i,orinet,authority,hub)
    return(a)
}
#子函数
conhk<-function(i,col,dat){
    co=as.character(col)
    tmp=dat[end==i,]
    tmp[,start:=as.character(start)]
    setkey(tmp,start)
    tmp=tmp[co,]
    tmp[is.na(hk),hk:=0]
    return(tmp$hk)
}
#主函数
con<-function(wdatbak,orinet,authority,hub){
    col=authority$col
    row=hub$row
    rlt_net=cbind(data.table(col),data.table(t(apply(data.table(col),1,conea,col,orinet,authority,hub))))
    for(i in col){
        setnames(rlt_net,paste("V",i,sep=""),paste(i))
    }
    dat=copy(wdatbak)
    setkey(dat,V10,userid)
    col=unique(dat$V10)
    row=unique(dat$userid)
    dat=data.table(dat$userid,dat$checkin_time,dat$V10)
    setkey(dat,V1,V2)
    dat[,con:=length(V2),by=V1]
    dat=dat[!(con==1),]
    dat[,c("start","end"):=list(shift(V3,1L),V3),by=V1]
    dat=dat[!is.na(start) & !(start==end),]
    dat=data.table(userid=dat$V1,start=dat$start,end=dat$end)
    hu=copy(hub)
    setkey(dat,userid)
    setkey(hu,row)
    dat=hu[dat,]
    dat[,hk:=sum(hub),by=.(start,end)]
    dat[,c("row","hub"):=list(NULL,NULL)]
    dat=unique(dat)
    rlt_net2=cbind(data.table(col),data.table(apply(data.table(col),1,conhk,col,dat)))
    for(i in col){
        setnames(rlt_net2,paste("V",i,sep=""),paste(i))
    }
    rlt_net=rlt_net+rlt_net2
    rlt_net[,col:=(col/2)]
    return(rlt_net)
}

##根据rltnet，求得从start出发经过count个点的路线
#主函数
cor<-function(rltnet,start,count){
    col=rltnet$col
    weight=0
    rlt=data.table(start)
    if(count>=1){
        for(i in 1:count){
            if(start==0) break
            negr=data.table(cbind(rltnet$col,t(rltnet[rltnet$col==start,])[2:(length(col)+1)]))
            setkey(negr,V2)
            negr=negr[!(negr$V2==0),]
            nextn=length(negr$V1)
            judge=TRUE
            while(judge){
                if(nextn==0) {
                    judge=TRUE
                    break
                }
                af=negr[nextn,V1]
                judge=FALSE
                start=af
                if(any(rlt==af)){
                    nextn=nextn-1
                    judge=TRUE
                }
            }
            if(!judge){
                weight=weight+t(rltnet[rltnet$col==rlt[i,start],])[start+1]
                rlt=rbind(rlt,data.table(start))
            }
            else{
                start=0
            }
        }
        k=length(rlt$start)
        if(k<(count+1)){
            for(i in (k+1):(count+1)){
                start=as.numeric(NA)
                rlt=rbind(rlt,data.table(start))
            }
        }
    }
    start=weight
    rlt=rbind(rlt,data.table(start))
    return(rlt)
}

##根据rltnet求得全程路线
#主函数
coar<-function(rltnet){
    col=rltnet$col
    rlt=data.table(col)
    rlt[,no:=col]
    rlt[,col:=NULL]
    no=0
    rlt=rbind(rlt,data.table(no))
    for(i in col){
        rltn=data.table(col)
        rltn[,start:=as.numeric(NA)]
        rltn[,col:=NULL]
        rltn[1]=i
        t=2
        tabo=data.table(col)
        tabo[,c("from","to"):=list(as.numeric(NA),as.numeric(NA))]
        tabo[,col:=NULL]
        cando=TRUE
        auth=0
        while(t<=length(col)){
            negr=data.table(cbind(rltnet$col,t(rltnet[rltnet$col==rltn[t-1,start],])[2:(length(col)+1)]))
            setkey(negr,V2)
            negr=negr[!(negr$V2==0),]
            nextn=length(negr$V1)
            judge=TRUE
            while(judge){
                if(nextn==0) {
                    judge=TRUE
                    break
                }
                af=negr[nextn,V1]
                judge=FALSE
                start=af
                tmp=rltn$start[1:(t-1)]
                if(any(tmp==af)){
                    nextn=nextn-1
                    judge=TRUE
                    next
                }
                tmp=tabo[!is.na(tabo$from) & !is.na(tabo$to) & tabo$from==rltn[t-1,start],to]
                if(!(length(tmp)==0)){
                    if(any(tmp==af)){
                        nextn=nextn-1
                        judge=TRUE
                        next
                    }
                }
            }
            if(!judge){
                if(t<length(col)){
                    for(k in (t):(length(col)-1)){
                        tabo[tabo$from==rltn[k,start] & tabo$to==rltn[k+1,start],c("from","to"):=list(NA,NA)]
                        rltn[k]=NA
                    }
                }
                rltn[length(col)]=NA
                rltn[t]=start
                auth=auth+negr[negr$V1==rltn[t,start],V2]
                t=t+1
            }
            else{
                if((t-1)<=1){
                    cando=FALSE
                    break
                }
                else{
                    t=t-1
                    from=rltn[t-1,start]
                    to=rltn[t,start]
                    tabo=rbind(tabo,cbind(data.table(from),data.table(to)))
                    auth=auth-t(rltnet[rltnet$col==rltn[t-1,start],])[rltn[t,start]+1]
                }
            }
        }
        if(cando){
            start=auth
            rltn=rbind(rltn,data.table(start))
            rlt=cbind(rlt,rltn)
            rlt[,paste("from_",i,sep=""):=start]
            rlt[,start:=NULL]
        }
    }
    rlt[,no:=NULL]
    return(rlt)
}

##根据rltnet，用户自定义多个区域x生成路线
#主函数
crfo<-function(rltnet,x){
    col=c(1:length(x))
    rltn=data.table(col)
    col=rltnet$col
    if(length(x)>=2){
        for(i in x){
            start=i
            rlt=data.table(start)
            for(j in 1:length(x)){
                if(start==0) break
                negr=data.table(cbind(rltnet$col,t(rltnet[rltnet$col==start,])[2:(length(col)+1)]))
                setkey(negr,V2)
                negr=negr[!(negr$V2==0),]
                nextn=length(negr$V1)
                judge=TRUE
                while(judge){
                    if(nextn==0){
                        judge==0
                        break
                    }
                    af=negr[nextn,V1]
                    start=af
                    judge=FALSE
                    if(!any(x==af)){
                        nextn=nextn-1
                        judge=TRUE
                        next
                    }
                    if(any(rlt==af)){
                        nextn=nextn-1
                        judge=TRUE
                        next
                    }
                }
                if(!judge){
                    rlt=rbind(rlt,data.table(start))
                }
                else{
                    start=0
                }
            }
            k=length(rlt$start)
            if(k<length(x)){
                for(m in (k+1):length(x)){
                    start=as.numeric(NA)
                    rlt=rbind(rlt,data.table(start))
                }
            }
            rltn=cbind(rltn,rlt)
            setnames(rltn,"start",paste("from_",i,sep=""))
        }
        rltn[,col:=NULL]
    }
    else{
        rltn=data.table(x)
        setnames(rltn,"x",paste("from_",x,sep=""))
    }
    return(rltn)
}

##主要处理函数
weibogo<-function(){
    drv=dbDriver("PostgreSQL")
    conn=dbConnect(drv,dbname="CityData",user="team",password="maet",host="120.25.253.38")
    place=dbGetQuery(conn,"select * from weibo_data")
    place=place$data
    season=c("_0","_1","_2","_3","_4")
    k=1
    for(i in place){
        for(j in season){
            if(i=="weibo" & j=="_0") next
            cat("[",k,"/179] Table ",i,j," begin! ",Sys.time(),"\n",sep="")
            #读取数据
            wdatbak=data.table(dbGetQuery(conn,paste("select * from ",i,j,sep="")))
            wdatbak=wdatbak[!is.na(longitude) & !is.na(latitude),]
            #windows下的汉字编码转换
            wdatbak$name=iconv(wdatbak$name,"UTF8","CP936")
            wdatbak$user_reg_place<-iconv(wdatbak$user_reg_place,"UTF8","CP936")
            wdatbak$address<-iconv(wdatbak$address,"UTF8","CP936")
            wdatbak$category<-iconv(wdatbak$category,"UTF8","CP936")
            wdatbak$shuxing<-iconv(wdatbak$shuxing,"UTF8","CP936")
            cat("\t","Readin Finished!","\n")
            oridat=data.table(cbind(wdatbak$userid,wdatbak$checkin_time,wdatbak$longitude,wdatbak$latitude))
            #筛选停留点
            stp=fsp(oridat)
            setnames(stp,c("V1","V2","V3","V4"),c("userid","checkin_time","longitude","latitude"))
            setkey(wdatbak,userid,checkin_time,longitude,latitude)
            setkey(stp,userid,checkin_time,longitude,latitude)
            wdatbak=wdatbak[stp,]
            output=data.table(poiindex=wdatbak$poiindex,userid=wdatbak$userid,user_reg_place=wdatbak$user_reg_place,checkin_time=wdatbak$checkin_time,name=wdatbak$name,address=wdatbak$address,category=wdatbak$category,shuxing=wdatbak$shuxing,categoryid=wdatbak$categoryid,longitude=wdatbak$longitude,latitude=wdatbak$latitude,type=wdatbak$type)
            dbWriteTable(conn,paste(i,j,"_filter",sep=""),output,overwrite = TRUE)
            wdatbak=wdatbak[type==1,]
            cat("\t","Filter Finished!","\n")
            #dbscan聚类
            eam=lfke(wdatbak)
            eps=eam[[1]]
            minpts=eam[[2]]
            wdatbak=wdbs(wdatbak,eps,minpts)
            output=data.table(poiindex=wdatbak$poiindex,userid=wdatbak$userid,user_reg_place=wdatbak$user_reg_place,checkin_time=wdatbak$checkin_time,name=wdatbak$name,address=wdatbak$address,category=wdatbak$category,shuxing=wdatbak$shuxing,categoryid=wdatbak$categoryid,longitude=wdatbak$longitude,latitude=wdatbak$latitude,regionid=wdatbak$V10)
            dbWriteTable(conn,paste(i,j,"_dbscan",sep=""),output,overwrite = TRUE)
            cat("\t","Dbscan Finished","\n")
            #HITS计算
            rlthits=hits(wdatbak)
            authority=rlthits[[1]]
            hub=rlthits[[2]]
            output=data.table(regionid=authority$col,authority=authority$authority)
            dbWriteTable(conn,paste(i,j,"_authority",sep=""),output,overwrite = TRUE)
            output=data.table(userid=hub$row,hub=hub$hub)
            dbWriteTable(conn,paste(i,j,"_hub",sep=""),output,overwrite = TRUE)
            cat("\t","Hits Finished!","\n")
            #路线生成
            orinet=cot(wdatbak)
            rltnet=con(wdatbak,orinet,authority,hub)
            output=copy(rltnet)
            setnames(output,"col","regionid")
            dbWriteTable(conn,paste(i,j,"_net",sep=""),output,overwrite = TRUE)
            cat("\t","Net Generation Finished!","\n")
            #生成默认路线
            roads=data.table(t(1:(length(authority$col)+1)))
            for(m in authority$col){
                road=cor(rltnet,m,(length(authority$col)-1))
                roads=rbind(roads,data.table(t(road)))
            }
            roads=roads[2:(length(authority$col)+1)]
            roads[,weight:=roads[[length(authority$col)+1]]]
            setkey(roads,weight)
            if(length(roads$V1)>=3){
                roads=data.table(t((roads[(length(authority$col)-2):length(authority$col)])))
                roads=roads[1:(length(authority$col)+1)]
            }
            else{
                roads=data.table(t(roads))
                roads=roads[1:(length(authority$col)+1)]
            }
            dbWriteTable(conn,paste(i,j,"_roads",sep=""),roads,overwrite = TRUE)
            cat("\t","Roads Generation Finished!","\n")
            #结束
            cat("[",k,"/179] Table ",i,j," finised! ",Sys.time(),"\n",sep="")
            k=k+1
            #清理
            rm(wdatbak,oridat,stp,eam,eps,minpts,rlthits,authority,hub,orinet,rltnet,road,roads)
            gc()
        }
    }
}

###函数定义结束

### RUN！
weibogo()
