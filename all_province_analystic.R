##前置需求
library(RPostgreSQL)
library(data.table)

apa1<-function(data){
    wdatbak=copy(data)
    wdatbak[,c("categoryid"):=list(as.numeric(categoryid))]
    setkey(wdatbak,categoryid)
    wdatbak[,c("con"):=length(address),by=categoryid]
    rlt=unique(data.table(cid=wdatbak$categoryid,con=wdatbak$con))
    rlt=rlt[con==max(con),]
    cidd=rlt$cid
    rltt=data.table()
    for(i in cidd){
        tmp=wdatbak[categoryid==i,]
        tmp[,c("poiindex"):=list(as.numeric(poiindex))]
        setkey(tmp,poiindex)
        tmp[,conn:=length(userid),by=poiindex]
        tmp2=unique(data.table(poiindex=tmp$poiindex,conn=tmp$conn,name=tmp$name,address=tmp$address,longitude=tmp$longitude,latitude=tmp$latitude))
        tmp2=tmp2[conn==max(conn),]
        tmp2[,c("cid","con","shuxing"):=list(i,rlt[cid==i,]$con,rlt[cid==i,]$shuxing)]
        rltt=rbind(rltt,tmp2,fill=TRUE)
    }
    return(rltt)   
}

apa2<-function(data){
    wdatbak=copy(data)
    wdatbak[,c("categoryid"):=list(as.numeric(categoryid))]
    setkey(wdatbak,categoryid)
    wdatbak[,c("con"):=length(address),by=categoryid]
    rlt=unique(data.table(cid=wdatbak$categoryid,con=wdatbak$con))
    ju=rlt[con==max(con),]
    if(any(ju$cid==4)){
        rlt=rlt[!(cid==4),]
    }
    rlt=rlt[con==max(con),]
    cidd=rlt$cid
    rltt=data.table()
    for(i in cidd){
        tmp=wdatbak[categoryid==i,]
        tmp[,c("poiindex"):=list(as.numeric(poiindex))]
        setkey(tmp,poiindex)
        tmp[,conn:=length(userid),by=poiindex]
        tmp2=unique(data.table(poiindex=tmp$poiindex,conn=tmp$conn,name=tmp$name,address=tmp$address,longitude=tmp$longitude,latitude=tmp$latitude))
        tmp2=tmp2[conn==max(conn),]
        tmp2[,c("cid","con","shuxing"):=list(i,rlt[cid==i,]$con,rlt[cid==i,]$shuxing)]
        rltt=rbind(rltt,tmp2,fill=TRUE)
    }
    return(rltt)   
}

apa3<-function(data){
    wdatbak=copy(data)
    wdatbak[,c("categoryid"):=list(as.numeric(categoryid))]
    setkey(wdatbak,categoryid)
    wdatbak[,c("con"):=length(address),by=categoryid]
    rlt=unique(data.table(cid=wdatbak$categoryid,con=wdatbak$con))
    rlt=rlt[!(cid==4) & !(cid==11)]
    rlt=rlt[con==max(con),]
    cidd=rlt$cid
    rltt=data.table()
    for(i in cidd){
        tmp=wdatbak[categoryid==i,]
        tmp[,c("poiindex"):=list(as.numeric(poiindex))]
        setkey(tmp,poiindex)
        tmp[,conn:=length(userid),by=poiindex]
        tmp2=unique(data.table(poiindex=tmp$poiindex,conn=tmp$conn,name=tmp$name,address=tmp$address,longitude=tmp$longitude,latitude=tmp$latitude))
        tmp2=tmp2[conn==max(conn),]
        tmp2[,c("cid","con","shuxing"):=list(i,rlt[cid==i,]$con,rlt[cid==i,]$shuxing)]
        rltt=rbind(rltt,tmp2,fill=TRUE)
    }
    return(rltt)   
}

aparun1<-function(){
    drv=dbDriver("PostgreSQL")
    conn=dbConnect(drv,dbname="CityData",user="team",password="maet",host="115.159.155.84")
    place=as.character(read.table("/home/sxyzy/ISPRS/weibo_data.csv",header=TRUE,sep=",")$table_name)
    time=c("_0","_1","_2","_3","_4")
    table1=data.table()
    table2=data.table()
    table3=data.table()
    for(i in place){
        for(j in time){
            if(i=="weibo_qita") next
            if(i=="weibo_all" & j=="_0") next
            if(i=="weibo_guangdong" & j=="_0") next
            #wdatbak=data.table(dbGetQuery(conn,paste("select * from ",i,j,"_dbscan",sep="")))
            wdatbak=data.table(read.table(paste("/home/sxyzy/citydatabak/",i,j,"_dbscan",".csv",sep=""),header=TRUE,sep=","))
            rlt1=apa1(wdatbak)
            rlt1[,c("place","time"):=list(substr(i,7,nchar(i)),substr(j,2,2))]
            table1=rbind(table1,rlt1,fill=TRUE)
            rlt2=apa2(wdatbak)
            rlt2[,c("place","time"):=list(substr(i,7,nchar(i)),substr(j,2,2))]
            table2=rbind(table2,rlt2,fill=TRUE)
            rlt3=apa3(wdatbak)
            rlt3[,c("place","time"):=list(substr(i,7,nchar(i)),substr(j,2,2))]
            table3=rbind(table3,rlt3,fill=TRUE)
            cat(i,j," finished!","\n",sep="")
        }
    }
    dbWriteTable(conn,"weibo_apa1", table1,col.names=TRUE,overwrite = TRUE)
    dbWriteTable(conn,"weibo_apa2", table2,col.names=TRUE,overwrite = TRUE)
    dbWriteTable(conn,"weibo_apa3", table3,col.names=TRUE,overwrite = TRUE)
}

apa4<-function(data){
    #browser()
    wdatbak=copy(data)
    row=nrow(wdatbak)
    wdatbak[,c("categoryid"):=list(as.numeric(categoryid))]
    setkey(wdatbak,categoryid)
    wdatbak[,c("con"):=length(address),by=categoryid]
    rlt=unique(data.table(cid=wdatbak$categoryid,con=wdatbak$con))
    rlt[,con:=con/row]
    setkey(rlt,cid)
    cidd=rlt$cid
    rltt=data.table()
    for(i in cidd){
        tmp=wdatbak[categoryid==i,]
        tmp[,c("poiindex"):=list(as.numeric(poiindex))]
        setkey(tmp,poiindex)
        tmp[,conn:=length(userid),by=poiindex]
        tmp2=unique(data.table(poiindex=tmp$poiindex,conn=tmp$conn,name=tmp$name,address=tmp$address,longitude=tmp$longitude,latitude=tmp$latitude))
        co=sort(unique(tmp2$conn),decreasing=TRUE)[1:3]
        for(j in co){
            tmp3=tmp2[conn==j,]
            tmp3[,c("cid","con","shuxing","conn"):=list(i,rlt[cid==i,]$con,rlt[cid==i,]$shuxing,conn/row)]
            rltt=rbind(rltt,tmp3,fill=TRUE)
        }
    }
    return(rltt)
}

aparun2<-function(){
    drv=dbDriver("PostgreSQL")
    conn=dbConnect(drv,dbname="CityData",user="team",password="maet",host="115.159.155.84")
    place=as.character(read.table("/home/sxyzy/ISPRS/weibo_data.csv",header=TRUE,sep=",")$table_name)
    time=c("_0","_1","_2","_3","_4")
    table1=data.table()
    for(i in place){
        for(j in time){
            if(i=="weibo_qita") next
            if(i=="weibo_all" & j=="_0") next
            if(i=="weibo_guangdong" & j=="_0") next
            #wdatbak=data.table(dbGetQuery(conn,paste("select * from ",i,j,"_dbscan",sep="")))
            wdatbak=data.table(read.table(paste("/home/sxyzy/citydatabak/",i,j,"_dbscan",".csv",sep=""),header=TRUE,sep=","))
            rlt1=apa4(wdatbak)
            rlt1[,c("place","time"):=list(substr(i,7,nchar(i)),substr(j,2,2))]
            table1=rbind(table1,rlt1,fill=TRUE)
            cat(i,j," finished!","\n",sep="")
        }
    }
    dbWriteTable(conn,"weibo_apa4", table1,col.names=TRUE,overwrite = TRUE)
}
