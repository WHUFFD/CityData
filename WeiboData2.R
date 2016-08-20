###前置需求
library(RPostgreSQL)
library(data.table)

###函数定义开始

##按星期计算
#子函数
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
#子函数
ttd<-function(yy,mo,dd){
    mo=mtd(mo)
    rlt=ifelse((yy-2014)==0,mo+dd,365+mo+dd)
    rlt=dtw(rlt)
    return(rlt)
}
#子函数
dtw<-function(rlt){
    rlt=rlt%%7
    if(rlt>4){
        rlt=rlt-5
    }
    else{
        rlt=rlt+2
    }
    return(rlt)
}
#主函数：按照时间计算一星期内签到人数
fsw<-function(oridat){
    wdat=copy(oridat)
    wdat[,c("yy","mo","dd","hh","mm","ss"):=list(as.numeric(substr(checkin_time,1,4)),as.numeric(substr(checkin_time,6,7)),as.numeric(substr(checkin_time,9,10)),as.numeric(substr(checkin_time,12,13)),as.numeric(substr(checkin_time,15,16)),as.numeric(substr(checkin_time,18,19)))]
    t=mapply(ttd,wdat$yy,wdat$mo,wdat$dd)
    wdat[,day:=t]
    wdat[,count:=length(checkin_time),by=.(day,regionid)]
    week=wdat[,list(regionid,day,count)]
    week=unique(week)
    setkey(week,regionid,day)
    days=c(0:6)
    rlt=data.table(days)
    for(i in unique(week$regionid)){
        dd=week[regionid==i,]
        setkey(dd,day)
        rlt=cbind(rlt,data.table(dd$count))
        setnames(rlt,"V1",paste(i))
    }
    return(rlt)
}
#主函数：计算一星期内各路线签到人数
fsrw<-function(oridat){
    wdat=copy(oridat)
    wdat[,c("yy","mo","dd","hh","mm","ss"):=list(as.numeric(substr(checkin_time,1,4)),as.numeric(substr(checkin_time,6,7)),as.numeric(substr(checkin_time,9,10)),as.numeric(substr(checkin_time,12,13)),as.numeric(substr(checkin_time,15,16)),as.numeric(substr(checkin_time,18,19)))]
    t=mapply(tts,wdat$yy,wdat$mo,wdat$dd,wdat$hh,wdat$mm,wdat$ss)
    wdat[,time:=t]
    t=mapply(ttd,wdat$yy,wdat$mo,wdat$dd)
    wdat[,day:=t]
    setkey(wdat,userid,time)
    wdat[,c("start","end"):=list(shift(regionid,1L),regionid),by=userid]
    wdat=wdat[!is.na(start) & !(start==end),]
    wdat[,count:=length(checkin_time),by=.(day,start,end)]
    dd=unique(wdat[,list(day,start,end,count)])
    setkey(dd,start,end,day)
    return(dd)
}

##找到每个区域的属性
lfci<-function(wdatbak){
    wdat=copy(wdatbak)
    wdat[,cir:=length(category),by=.(regionid,categoryid)]
    wdat[,cia:=length(category),by=categoryid]
    wdat[,cra:=length(category),by=regionid]
    wdat[,tf:=(1+log10(cir/cra))]
    al=length(wdat$category)
    wdat[,idf:=log10(al/(1+cia))]
    wdat[,tfidf:=tf*idf]
    cattab=wdat[,list(shuxing,categoryid,regionid,tfidf)]
    cattab[,maxtfidf:=max(tfidf),by=regionid]
    caterlt=cattab[tfidf==maxtfidf,]
    caterlt=unique(caterlt[,list(regionid,categoryid,shuxing)])
    return(caterlt)
}

##main
weibogo<-function(){
    drv=dbDriver("PostgreSQL")
    conn=dbConnect(drv,dbname="CityData",user="team",password="maet",host="120.25.253.38")
    place=dbGetQuery(conn,"select * from weibo_data_copy")
    place=place$data
    t=c("_0","_1","_2","_3","_4")
    k=1
    for(i in place){
        for(j in t){
            if(i=="weibo_all" & j=="_0") next
            #读取数据
            cat("[",k,"/165]","Table ",i,j," begin!","\n",sep="")
            wdatbak=data.table(dbGetQuery(conn,paste("select * from ",i,j,"_dbscan",sep="")))
            cat("\t","Readin finished!","\n")
            caterlt=lfci(wdatbak)
            cat("\t","Categoryid finished","\n")
            oridat=wdatbak[,list(checkin_time,regionid)]
            week=fsw(oridat)
            cat("\t","Week Region finished","\n")
            oridat=wdatbak[,list(userid,checkin_time,regionid)]
            rlt=fsrw(oridat)
            cat("\t","Week Net finished","\n")
            dbWriteTable(conn,paste(i,j,"_categoryid",sep=""),caterlt,overwrite=TRUE)
            dbWriteTable(conn,paste(i,j,"_week_region",sep=""),week,overwrite=TRUE)
            dbWriteTable(conn,paste(i,j,"_week_net",sep=""),rlt,overwrite=TRUE)
            cat("\t","Writein finished","\n")
            cat("[",k,"/165]","Table ",i,j," finished!","\n",sep="")
            k=k+1
        }
    }
    cat("All finished!")
}

###函数定义结束

### RUN!
weibogo()
