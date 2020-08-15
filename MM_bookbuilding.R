rm(list=ls());gc()
library(tidyverse)
library(data.table)
library(lubridate)
library(rlist)
library(foreach)
library(doParallel)
setwd("C:/Users/Eric/Desktop/FinanceManagement/MicroMarket/HW2/OrderBook")
#==========Functions=========================#
LOB=function(x){
record = data.frame()
for(i in 1:nrow(x)){
  temp = data.frame(Price = x$OdrPr[i],
                    Bid_vol = ifelse(x$BuySell[i] == 'B', x$OdrShr[i], 0),
                    Ask_vol = ifelse(x$BuySell[i] == 'S', x$OdrShr[i], 0))
  record = record %>% rbind(temp)}
n = nrow(temp)
Trade_Table = record %>%
  group_by(Price) %>%
  summarise(total_Bid_vol = sum(Bid_vol), total_Ask_vol = sum(Ask_vol))%>%
  arrange(desc(Price))
return(Trade_Table)}
#----------Transaction-----------------------------------------#
Matching = function(Trade_Table, DPr){
Trade_Table = Trade_Table %>%
  mutate(Bid_cum = cumsum(total_Bid_vol), Ask_cum = cumsum(Trade_Table$total_Ask_vol[nrow(Trade_Table):1])[nrow(Trade_Table):1])
Trade_Table = Trade_Table %>% 
  mutate(Deal_vol = ifelse(Bid_cum >= Ask_cum, Ask_cum, Bid_cum), surplus = Bid_cum - Ask_cum)
if(! max(Trade_Table$Deal_vol) == 0){
  if(length(which(Trade_Table$Deal_vol == max(Trade_Table$Deal_vol)))>1){
    Deal_Price = Trade_Table %>%
      filter(Deal_vol == max(Trade_Table$Deal_vol)) %>% filter(abs(surplus)==min(abs(surplus)))%>%
      filter(Price == Price[which.min(abs(Price-DPr))]) %>%  slice(1) %>% 
      select(Price) %>% as.numeric()
    Deal_volume = Trade_Table$Deal_vol[which(Trade_Table$Price == Deal_Price)]
  }else{
    Deal_Price = Trade_Table$Price[which.max(Trade_Table$Deal_vol)]
    Deal_volume = Trade_Table$Deal_vol[which(Trade_Table$Price == Deal_Price)]}
  if(Trade_Table$surplus[which(Trade_Table$Price == Deal_Price)] >= 0){
    if(which(Trade_Table$Price == Deal_Price)<1){
      Trade_Table$total_Ask_vol[which(Trade_Table$Price == Deal_Price):nrow(Trade_Table)] = 0
      Trade_Table$total_Bid_vol[which(Trade_Table$Price == Deal_Price)]  = Trade_Table$Bid_cum[which(Trade_Table$Price == Deal_Price)] - Deal_volume 
    }else{
    Trade_Table$total_Ask_vol[which(Trade_Table$Price == Deal_Price):nrow(Trade_Table)] = 0
    Trade_Table$total_Bid_vol[1:(which(Trade_Table$Price == Deal_Price)-1)] = 0
    Trade_Table$total_Bid_vol[which(Trade_Table$Price == Deal_Price)]  = Trade_Table$Bid_cum[which(Trade_Table$Price == Deal_Price)] - Deal_volume 
}  }else{
    if((which(Trade_Table$Price == Deal_Price)+1)>nrow(Trade_Table)){
      Trade_Table$total_Bid_vol[(1:which(Trade_Table$Price == Deal_Price))] = 0
      Trade_Table$total_Ask_vol[which(Trade_Table$Price == Deal_Price)]  = Trade_Table$Ask_cum[which(Trade_Table$Price == Deal_Price)] - Deal_volume 
    }else{
     Trade_Table$total_Ask_vol[(which(Trade_Table$Price == Deal_Price)+1):nrow(Trade_Table)] = 0
     Trade_Table$total_Bid_vol[(1:which(Trade_Table$Price == Deal_Price))] = 0
     Trade_Table$total_Ask_vol[which(Trade_Table$Price == Deal_Price)]  = Trade_Table$Ask_cum[which(Trade_Table$Price == Deal_Price)] - Deal_volume 
}
    }}else{
    Deal_Price = DPr
    Deal_volume = 0 
    Trade_Table = Trade_Table}
Trade_Table = Trade_Table %>% filter(!(total_Ask_vol==0 & total_Bid_vol == 0))
tradetemp = list(Table = Trade_Table %>% select(Price, total_Ask_vol, total_Bid_vol), Price = Deal_Price, Volume = Deal_volume)
return(tradetemp)
}
#---------------get values-----------------------------#
Bid_Ask_side = function(x, request){
  x= x %>% arrange(desc(Price))
  if(request == 'B.P'){
  temp = x$Price[which(x$total_Bid_vol > 0)] %>% sort(decreasing = T) }else if(request == 'A.P'){
  temp = x$Price[which(!x$total_Ask_vol > 0)] %>% sort() }else if(request == 'B.V'){
    temp = x %>% filter(total_Bid_vol > 0) %>% arrange(desc(Price)) %>% select(total_Bid_vol)%>% unlist() %>% as.numeric()
  }else if(request == 'A.V'){
    temp = x %>% filter(total_Ask_vol > 0) %>% arrange(Price) %>% select(total_Ask_vol)%>% unlist() %>% as.numeric()
    }
  return(temp)}
#--------------DATA---------------------------------#
GetData = function(xx){
  bidP = Bid_Ask_side(xx[[1]],'B.P')
  askP = Bid_Ask_side(xx[[1]],'A.P')
  bidV = Bid_Ask_side(xx[[1]],'B.V')
  askV = Bid_Ask_side(xx[[1]],'A.V')
  df = data.frame(Deal_Price = xx[[2]],Deal_volume = xx[[3]],
                  Best_Bid = bidP[1],Best_Ask = askP[1],Best2_Bid = bidP[2],Best2_Ask = askP[2],
                  Best3_Bid = bidP[3],Best3_Ask = askP[3],Best4_Bid = bidP[4],Best4_Ask = askP[4], 
                  Best5_Bid = bidP[5],Best5_Ask = askP[5],Best_Bid_vol = bidV[1],Best_Ask_vol = askV[1],
                  Best2_Bid_vol = bidV[2],Best2_Ask_vol = askV[2],Best3_Bid_vol = bidV[3],Best3_Ask_vol = askV[3],
                  Best4_Bid_vol = bidV[4],Best4_Ask_vol = askV[4], Best5_Bid_vol = bidV[5],Best5_Ask_vol = askV[5])
  return(df)}
#==============Main=====================================#
Files =list.files()[seq(1,59,2)]

df = data.frame()
cl = makeCluster(10)
registerDoParallel(cl)

tradedf=foreach( iy = 1:length(Files),.combine = 'rbind',.packages = c('rlist','dplyr','lubridate','data.table'))%dopar%{
data = fread(Files[iy],data.table = F)[,-1]
data = data %>% select(OdrDate,OdrTime,BuySell,StkNo,OdrPr,OdrShr)
#----------setting-------------------------------#
data$OdrDate = ymd(data$OdrDate)
date_list = unique(data$OdrDate) 
  df = data.frame()
for(ix in 1:length(date_list)){
  data_temp = data %>% filter(OdrDate == date_list[ix])
  data_temp = data_temp %>% filter(OdrTime%/%10000<1340) %>%#去除零股委託單
    mutate(sec  = OdrTime%%10000%/%100,
           hourmin = OdrTime%/%10000) %>%
    mutate(itvl = sec%/%30) 
  data_open = data_temp %>% filter(hourmin<900)  
  data_close = data_temp %>% filter(hourmin>=1325)
  data_intra = data_temp %>% filter(hourmin<1325 & hourmin>=900) %>% mutate(interval = hourmin*100)
  itvl_list =unique(data_intra$interval)
  data_itvl = list()
  for(i in itvl_list){
    temp = data_intra %>% filter(interval == i)
    data_itvl = list.append(data_itvl,temp)}
#Trading Process

  intra_temp = LOB(data_open) %>% Matching(.,DPr = 0)#改成GetDATA
  df = df %>% rbind(data.frame(code = Files[iy],date = date_list[ix] ,itvl = 85900) %>% cbind(GetData(intra_temp)))
  for(i in 1:length(itvl_list)){
  intra_temp=LOB(data_itvl[[i]]) %>% rbind(intra_temp[[1]])%>% group_by(Price)%>%
    summarise(total_Bid_vol = sum(total_Bid_vol), total_Ask_vol = sum(total_Ask_vol)) %>%
    arrange(desc(Price)) %>% Matching(.,intra_temp[[2]])
  
  df = df %>% rbind(data.frame(code = Files[iy],date = date_list[ix] ,itvl = itvl_list[i]) %>% cbind(GetData(intra_temp)))}
  
  if(nrow(data_close)==0){next}else{
  intra_temp=LOB(data_close) %>% rbind(intra_temp[[1]])%>% group_by(Price)%>%
    summarise(total_Bid_vol = sum(total_Bid_vol), total_Ask_vol = sum(total_Ask_vol)) %>%
    arrange(desc(Price))%>% Matching(.,intra_temp[[2]])
  df = df %>% rbind(data.frame(code = Files[iy],date = date_list[ix] ,itvl = 133000) %>% cbind(GetData(intra_temp)))
  }
  }
return(df)
}
stopCluster(cl)

write.csv(tradedf,'matcheddata_halfmin.csv')

#count()

data2 = data %>% group_by(OdrNo2) %>% filter(n()>1)
list =unique(data2 %>% filter(ChangedCode %in% c(4,6)) %>% select(OdrNo2) %>% unlist() %>% as.numeric())
data3 = data %>% group_by(OdrNo2) %>% filter(n()>1) %>% filter(OdrNo2 %in% list )%>%  summarise(total = sum(OdrShr))






  
  
