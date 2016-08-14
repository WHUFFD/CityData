#前置需求
library(RPostgreSQL)
library(data.table)
library(dplyr)
library(rgeos)
library(rgdal)
library(fpc)

#函数定义开始

#月份转化为截至上个月底的经过时间
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

#筛选停留点
fsp<-function(oridat){
    wdat=copy(oridat)
    wdat[,c("yy","mo","dd","hh","mm","ss"):=list(as.numeric(substr(V2,1,4)),as.numeric(substr(V2,6,7)),as.numeric(substr(V2,9,10)),as.numeric(substr(V2,12,13)),as.numeric(substr(V2,15,16)),as.numeric(substr(V2,18,19)))]
    wdat[,c("time","type","dtime","dis","avgS"):=list(as.numeric(NA),as.numeric(NA),as.numeric(NA),as.numeric(NA),as.numeric(NA))]
    for(i in 1:length(wdat$V2)){
        mon=wdat[i]$mo
        mon=mtd(mon)
        wdat[i]$time=((ifelse((wdat[i]$yy-2014)==0,mon+wdat[i]$dd,365+mon+wdat[i]$dd)*24*3600+wdat[i]$hh*3600+wdat[i]$mm*60+wdat[i]$ss))
    }
    wdat[,c("V3","V4"):=list(as.numeric(V3),as.numeric(V4))]
    wdat <- data.frame(wdat, x=wdat$V3, y=wdat$V4)
    coordinates(wdat) <- c("x", "y")
    proj4string(wdat) <- CRS("+init=epsg:4326 +proj=longlat +ellps=WGS84 + datum=WGS84")
    wdat <- spTransform(wdat, CRS("+init=epsg:2414 +datum=WGS84"))
    wdat <- data.table(coordinates(wdat), wdat@data)
    row=unique(wdat$V1)
    rlt=data.table(wdat[1])
    rlt[,V1:=0]
    for(i in row){
        usck=wdat[V1==i,]
        if(length(usck$V3)==1){
            usck[,c("type","dtime","dis","avgS"):=list(1,0,0,0)]
            rlt=rbind(rlt,usck)
            next
        }
        if(length(usck$V3)==2){
            usck[,c("type","dtime","dis","avgS"):=list(1,0,0,0)]
            rlt=rbind(rlt,usck)
            next
        }
        setkey(usck,time)
        usck[,dtime:=(time-shift(time,1L))]
        usck[,dis:=sqrt((x-shift(x,1L))*(x-shift(x,1L)) + ((y-shift(y,1L))*(y-shift(y,1L))))]
        usck[,avgS:=(dis/dtime)]
        usck[is.na(dtime) & is.na(dis),c("dtime","dis","avgS"):=list(0,0,0)]
        usck[,type:=ifelse(avgS>0.6,2,1)]
        usck[1,type:=usck[2,type]]
        start=1
        end=1
        t=usck[1,type]
        while(!(end==(length(usck$V1)+1))){
            if(end==length(usck$V1)){
                end=end+1
            }
            else if((usck[end,type]==t)){
                end=end+1
                next
            }
            if(t==1){
                st=sum(usck[start:(end-1),dtime])
                if(st<=30){
                    usck[start:(end-1),type:=2]
                }
            }
            else if(t==2){
                sd=sum(usck[start:(end-1),dis])
                if(sd<=200){
                    usck[start:(end-1),type:=1]
                }
            }
            start=end
            t=usck[end,type]
        }
        rlt=rbind(rlt,usck)
    }
    rlt=rlt[!(V1==0),]
    return(rlt)
}

#根据数据计算最佳dbscan聚类参数
lfke<-function(wdatbak){
    x=unique(wdatbak[,list(longitude,latitude)])
    disa=data.table()
    for(i in 1:length(x$longitude)){
        lng1=x[i]$longitude
        lat1=x[i]$latitude
        x[,dis:=dista(lng1,lat1,longitude,latitude)]
        disa=rbind(disa,data.table(t(x$dis)))
    }
    for(i in 1:length(x$longitude)){
        setnames(disa,paste("V",i,sep=""),paste(i))
    }
    disb=data.table()
    for(i in 1:length(x$longitude)){
        s=data.table(t(disa[i,]))$V1
        s=sort(s)
        disb=rbind(disb,data.table(t(s)),fill=TRUE)
    }
    epstab=data.table()
    for(i in 1:length(x$longitude)){
        t=disb[[i]]
        t=sum(t)/length(t)
        epstab=rbind(epstab,data.table(t))
    }
    setnames(epstab,"t","eps")
    epstab[,c("areas","trash"):=list(as.numeric(NA),as.numeric(NA))]
    for(i in 1:40){
        k=30
        exp=epstab[i,eps]
        da=wdatbak[,list(longitude,latitude)]
        ds=dbscan(da,exp,k)
        a=(length(unique(ds$cluster))-1)
        t=length(which(ds$isseed==FALSE))
        epstab[i,c("areas","trash"):=list(a,t)]
    }
    epstab=epstab[!is.na(areas),]
    for(i in 2:29){
        t1=epstab[i-1,]$areas
        t2=epstab[i+1,]$areas
        t=(t2-t1)/2
        epstab[i,da:=t]
        t1=epstab[i-1,]$trash
        t2=epstab[i+1,]$trash
        t=(t2-t1)/2
        epstab[i,dt:=t]
    }
    p=epstab[!is.na(dt),]
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
    x[,c("dis","co"):=list(NULL,as.numeric(NA))]
    for(i in 1:length(x$longitude)){
        dd=copy(wdatbak)
        tmp=disa[[i]]
        tmp=which(tmp<=exp)
        k=x[tmp,]
        setkey(dd,longitude,latitude)
        setkey(k,longitude,latitude)
        dd=dd[k,]
        x[i,co:=(count(dd)$n)]
    }
    minpts=sum(x$co)/length(wdatbak$longitude)
    return(list(eps,minpts))
}

#dbscan聚类
wdbs<-function(rs,eps,minpts){
    x=rs[,list(longitude,latitude)]
    ds=dbscan(x,eps,minpts)
    xs=ds$cluster
    qynum=max(xs)
    agr=data.table(rs[1])
    agr[,userid:=NA]
    agr[,V10:=0]
    for(i in 1:qynum){
        sc=which(xs==i)
        fc=rs
        fc[,V10:=i]
        sss=fc[sc,]
        agr=rbind(agr,sss)
    }
    agr=na.omit(agr)
    return(agr)
}

#HITS算法，根据签到点计算hub和authority
hits<-function(wdatbak){
    setkey(wdatbak,V10,userid)
    col=unique(wdatbak$V10)
    row=unique(wdatbak$userid)
    tabn=data.table(row)
    for(i in row){
        for(j in col){
            tabn[row == i,paste(j):=count(wdatbak[userid == i & V10 == j,])]
        }
    }
    hub=data.table(row)
    authority=data.table(col)
    hub[,hub:=1]
    authority[,authority:=1]
    a_old=authority$col
    h_old=hub$row
    down_a_old=0
    down_h_old=0
    jugg=0
    i=1
    while(TRUE){
        authority[,authority:=0]
        for(m in row){
            auth=tabn[row == m,]
            auth[,row:=as.numeric(row)]
            auth=t(auth*hub[row == m,hub])[2:(length(col)+1)]
            authority[,authority:=(authority+auth)]
        }
        down_a=authority[,authority]
        down_a=down_a*down_a
        down_a=sqrt(sum(down_a))
        authority[,authority:=(authority/down_a)]
        hub[,hub:=0]
        for(m in col){
            hu=tabn[[m+1]]*authority[col==m,authority]
            hub[,hub:=(hub+hu)]
        }
        down_h=hub[,hub]
        down_h=down_h*down_h
        down_h=sqrt(sum(down_h))
        hub[,hub:=(hub/down_h)]
        i=i+1
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
        down_a_old=down_a
        down_h_old=down_h
    }
    return(list(authority,hub))
}

#求得初始区域间网络
cot<-function(wdatbak){
    col=unique(wdatbak$V10)
    row=unique(wdatbak$userid)
    ori_net=data.table(col)
    for(i in col){
        ori_net[,paste(i):=0]
    }
    for(j in row){
        user_ct=data.table(cbind(wdatbak[wdatbak$userid==j,]$checkin_time,wdatbak[wdatbak$userid==j,]$V10))
        setkey(user_ct,V1)
        area=as.numeric(user_ct$V2)
        k=length(area)
        if(k==1) next
        a1=area[1]
        for(a2 in area[2:k]){
            if(!(a2==a1)){
                ori_net[col==a1,][[a2+1]]=ori_net[col==a1,][[a2+1]]+1
            }
            a1=a2
        }
    }
    return(ori_net)
}

#求得最终区域间网络
con<-function(wdatbak,orinet,authority,hub){
    col=authority$col
    row=hub$row
    rlt_net=data.table(col)
    for(i in col){
        rlt_net[,paste(i):=0]
    }
    for(i in col){
        for(j in col){
            if(orinet[col==i,][[j+1]]==0) next
            outn=sum(orinet[col==i,])-i
            inn=sum(orinet[[j+1]])
            a=orinet[col==i,][[j+1]]*((orinet[col==i,][[j+1]]/outn)*authority[col==i,authority]+(orinet[col==i,][[j+1]]/inn)*authority[col==j,authority])
            rlt_net[col==i,][[j+1]]=a
        }
    }
    for(j in row){
        user_ct=data.table(cbind(wdatbak[wdatbak$userid==j,]$checkin_time,wdatbak[wdatbak$userid==j,]$V10))
        setkey(user_ct,V1)
        area=as.numeric(user_ct$V2)
        k=length(area)
        if(k==1) next
        a1=area[1]
        for(a2 in area[2:k]){
            if(!(a2==a1)){
                rlt_net[col==a1,][[a2+1]]=rlt_net[col==a1,][[a2+1]]+hub[row==j,hub]
            }
            a1=a2
        }
    }
    return(rlt_net)
}

#根据rltnet求得全程路线
coar<-function(rltnet){
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
                for(k in (t):(length(col)-1)){
                    tabo[tabo$from==rltn[k,start] & tabo$to==rltn[k+1,start],c("from","to"):=list(NA,NA)]
                    rltn[k]=NA
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
            rltn=rbind(rlt,data.table(start))
            rlt=cbind(rlt,rltn)
            rlt[,paste("from_",i,sep=""):=start]
            rlt[,start:=NULL]
        }
    }
    rlt[,no:=NULL]
    return(rlt)
}

#根据rltnet，求得从start出发经过count个点的路线
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

#主要处理函数
weibogo<-function(){
    drv=dbDriver("PostgreSQL")
    conn=dbConnect(drv,dbname="CityData",user="team",password="maet",host="120.25.253.38")
    place=dbGetQuery(conn,"select * from weibo_data")
    place=place$data
    t=c("","_1","_2","_3","_4")
    for(i in place){
        for(j in t){
            if(i=="weibo" & j=="") next
            #读取数据
            drv=dbDriver("PostgreSQL")
            conn=dbConnect(drv,dbname="CityData",user="team",password="maet",host="120.25.253.38")
            wdatbak=data.table(dbGetQuery(conn,paste("select * from ",i,j,sep="")))
            wdatbak=wdatbak[!is.na(longitude) & !is.na(latitude),]
            #windows下的汉字编码转换
            #wdatbak$name=iconv(wdatbak$name,"UTF8","CP936")
            #wdatbak$user_reg_place<-iconv(wdatbak$user_reg_place,"UTF8","CP936")
            #wdatbak$address<-iconv(wdatbak$address,"UTF8","CP936")
            #wdatbak$category<-iconv(wdatbak$category,"UTF8","CP936")
            oridat=data.table(cbind(wdatbak$userid,wdatbak$checkin_time,wdatbak$longitude,wdatbak$latitude))
            #筛选停留点
            stp=fsp(oridat)
            setnames(stp,c("V1","V2","V3","V4"),c("userid","checkin_time","longitude","latitude"))
            setkey(wdatbak,userid,checkin_time,longitude,latitude)
            setkey(stp,userid,checkin_time,longitude,latitude)
            wdatbak=wdatbak[stp,]
            wdatbak=wdatbak[type==1,]
            #dbscan聚类
            eam=lfke(wdatbak)
            eps=eam[[1]]
            minpts=eam[[2]]
            wdatbak=wdbs(wdatbak,eps,minpts)
            dbWriteTable(conn,paste(i,j,"_dbscan",sep=""),wdatbak,overwrite = TRUE)
            #HITS计算
            rlthits=hits(wdatbak)
            authority=rlthits[[1]]
            hub=rlthits[[2]]
            dbWriteTable(conn,paste(i,j,"_authority",sep=""),authority,overwrite = TRUE)
            dbWriteTable(conn,paste(i,j,"_hub",sep=""),hub,overwrite = TRUE)
            #路线生成
            orinet=cot(wdatbak)
            rltnet=con(wdatbak,orinet,authority,hub)
            dbWriteTable(conn,paste(i,j,"_net",sep=""),rltnet,overwrite = TRUE)
            #生成默认路线
            roads=data.table(t(1:(length(authority$col)+1)))
            for(i in authority$col){
                road=cor(rltnet,i,(length(authority$col)-1))
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
            #结束
            cat("Table ",i,j," finised! ","\n",sep="")
            #清理
            rm(wdatbak,oridat,stp,eam,eps,minpts,rlthits,authority,hub,orinet,rltnet,road,roads)
            gc()
        }
    }
}

#函数定义结束

# RUN！
weibogo()
