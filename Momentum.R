rm(list = ls());gc()
library(lubridate)
library(tidyverse)
library(data.table)
library(spectral)
library(tibbletime)
library(ggplot2)
library(stocks)
setwd("D:/Finance_Management/Micro_Market/Final")
#==============================================#
FFT_lowpass = function(x,BandWidth){
  y = filter.fft(x,BW = BandWidth)
  return(Re(y))
}
GH_ratio = function(x){
  y = last(x)/max(x)
  return(y)
}
Prod_Ret = function(x){
  y = prod(1+x/100,na.rm = T)-1
  return(y)
}
quanclass = function(x, group_Num){
  q_statistics = quantile(x,probs = 1:group_Num/group_Num,na.rm = T)
  vec = c()
  for(i in 1:length(x)){
    particle = 1
    for(iy in 1:(group_Num-1)){
      if(x[i]>q_statistics[iy]){
        particle = iy+1
      }
    }
    vec = c(vec,particle)
  }
  return(vec)
}
sortWeek = function(x){
  Date = ymd(x)
  y = (Date - ymd(20161231))%/%7
  return(y)
}
cost_function = function(x){
  y = (1-(0.001425*2+0.003)/6)*x
  return(y)
}

max_Cont = function(x,stream){
  temp_neg = c()
  temp_pos = c()
  counts_pos = 0
  counts_neg = 0
  for(i in 1:length(x)){
    if(x[i]<0){
      counts_pos = 0
      counts_neg = counts_neg+1
    }else{
      counts_neg = 0
      counts_pos = counts_pos+1
    }
    temp_neg = c(temp_neg,counts_neg)
    temp_pos = c(temp_pos,counts_pos)
  }
  if(stream == 'Win'){
    return(max(temp_pos))
  }else if(stream == 'Loss'){
    return(max(temp_neg))
  }
}
Profit_factor = function(x){
  Total_Profit = 0
  Total_Loss = 0
  for(i in 1:length(x)){
    if(x[i]>=0){
      Total_Profit = Total_Profit+x[i]
    }else{
      Total_Loss = Total_Loss-x[i]
    }
  }
  y = Total_Profit/Total_Loss
  return(y)
}

#==============================================#
DATA = fread('Price_data.csv',data.table = F)

colnames(DATA) = c('StkNo','Name','Ind','Date','Open','High','Low',
                   'Close','Volume','d_Volume','Ret','Turnover')
factors = fread('factors.csv',data.table = F)

colnames(factors) = c('StkNo','Name','Date','SMB','Market','UMD','Rf','HML')

DATA = DATA %>%
  group_by(StkNo) %>% filter(n()>700) %>%
  ungroup()

DATA = DATA 
DATA$Date = ymd(DATA$Date)

Stocklist = unique(DATA$StkNo)

i=1
GH_TABLE = data.frame()

for(i in 1:length(Stocklist)){
  print(paste(i,'/',length(Stocklist)))
  temp = DATA %>% filter(StkNo == Stocklist[i])
  Weeklist = temp %>%
    filter(Date>=ymd(20180101)) %>% 
    mutate(wkday = weekdays(Date)) %>%
    filter(wkday=='星期五') %>%
    select(Date) %>%
    mutate(Date = as.character(Date)) %>% 
    unlist() %>% unique()
  df = data.frame()
  for(iy in 1:length(Weeklist)){
    tp = temp %>% filter(Date<=ymd(Weeklist[iy]),Date>ymd(Weeklist[iy])-365)
    return.df = data.frame(StkNo = Stocklist[i],
                           Date = Weeklist[iy],
                           GH_R = GH_ratio(tp$Close),
                           Price = last(tp$Close),
                           Days = ymd(tp$Date[which.max(tp$Close)])-ymd(Weeklist[iy]))
   
    df = df %>% rbind(return.df)
  }
  GH_TABLE = GH_TABLE %>% rbind(df)
}

Future_TABLE = data.frame()

for(i in 1:length(Stocklist)){
  print(paste(i,'/',length(Stocklist)))
  temp = DATA %>% filter(StkNo == Stocklist[i])
  Weeklist = temp %>%
    filter(Date>=ymd(20180101),Date<=ymd(20191229)-42) %>% 
    mutate(wkday = weekdays(Date)) %>%
    filter(wkday=='星期五') %>%
    select(Date) %>%
    mutate(Date = as.character(Date)) %>% 
    unlist() %>% unique()
  if(length(Weeklist)==0){next}
  df = data.frame()
  for(iy in 1:length(Weeklist)){
    tp = temp %>% filter(Date<=ymd(Weeklist[iy])+42,Date>ymd(Weeklist[iy])+7)
    return.df = data.frame(StkNo = Stocklist[i],
                           Date = Weeklist[iy],
                           BuyPrice = first(tp$Close),
                           SellPrice = last(tp$Close),
                           Future_Ret = Prod_Ret(tp$Ret))
    df = df %>% rbind(return.df)
  }
  Future_TABLE = Future_TABLE %>% rbind(df)
}

exp_Data =  GH_TABLE %>%
  left_join(Future_TABLE,by = c('StkNo','Date')) #%>% 

exp_Data = exp_Data %>%
  na.omit() %>% 
  filter(!is.nan(Future_Ret)) %>% 
  group_by(Date) %>% 
  mutate(GH_group = quanclass(GH_R,5)) %>% 
  group_by(GH_group,Date) %>% 
  mutate(distance_group = quanclass(Days,3))

exp_sum = exp_Data %>%
  group_by(Date,GH_group,distance_group) %>%
  summarise(sRet = ((1+mean(Future_Ret,na.rm = T))**(1/6)-1)*100) %>% 
  ungroup() %>% 
  group_by(GH_group,distance_group) %>% 
  summarise(Ret = mean(sRet,na.rm = T)) %>% 
  spread(key = 'distance_group',value = 'Ret')

#===========================================================#
expday_sum = exp_Data %>%
  group_by(Date,GH_group,distance_group) %>%
  summarise(sRet = ((1+mean(Future_Ret,na.rm = T))**(1/6)-1)*100) %>% 
  ungroup() %>% 
  filter(distance_group==3) %>% 
  group_by(Date) %>% 
  summarise(PortfolioRet = sRet[which(GH_group==5)]-sRet[which(GH_group==1)]) %>% 
  mutate(cumRet = cumprod(cost_function(1+PortfolioRet/100)))

#===========================================================#
exp_sum = exp_Data %>%
  group_by(Date,GH_group,distance_group) %>%
  summarise(Ret = (1+mean(Future_Ret,na.rm = T)**(1/6)-1)*100)%>%
  ungroup() %>%
  group_by(GH_group,distance_group) %>%
  summarise(Ret = mean(Ret,na.rm = T)) %>% 
  spread(key = 'distance_group',value = 'Ret')

factors$Date = ymd(factors$Date)
expday_sum$Date = ymd(expday_sum$Date)+42


FAMA = expday_sum %>%
  left_join(factors,by = 'Date') %>%
  mutate(PortfolioRet_Rf = PortfolioRet-((1+Rf/100)**(1/52)-1)) %>% 
  mutate(Market = ifelse(is.na(Market),0,Market)) %>% 
  mutate(cumMarket = cumprod(1+Market/100))

ggplot(data = FAMA,aes(x = ymd(Date),y = cumMarket,col = 'Market')) +
  geom_line(size = 1.2) +
  labs(x = 'Date',y='One dollar in the beginning of 2018')+ggtitle('Momentum Strategy') +
  theme(plot.background = element_rect(fill = "#BFD5E3"))+
  geom_line(aes(x = ymd(Date),y = cumRet, col = 'Portfolio'),size = 1.2)+
  scale_color_manual(values = c('Portfolio'='red','Market'='blue'))




Reg = lm(data = FAMA, formula = PortfolioRet_Rf~Market+SMB+HML+UMD)
summary(Reg)
#====================Result==============================#
Win_Ret = length(which((FAMA$PortfolioRet)>0))/nrow(FAMA)
MDD =mdd(FAMA$cumRet)
ContLossWeeks = max_Cont(FAMA$PortfolioRet,stream = 'Loss')
ContWinWeeks = max_Cont(FAMA$PortfolioRet,stream = 'Win')
Profit_fac = Profit_factor(FAMA$PortfolioRet)
Avg_Profit_Loss = FAMA %>%
  mutate(WinLoss = ifelse(PortfolioRet>=0,'+','-')) %>%
  group_by(WinLoss) %>% summarise(Avg_Scale = mean(PortfolioRet))
#=======================================================#
strategy_Data = exp_Data %>%
  filter(GH_group%in%c(1,5),distance_group ==3) %>% 
  mutate(Profit_de_trade = ifelse(GH_group==5,
                                  (1-0.004425)*SellPrice-(1+0.001425)*BuyPrice,
                                  (1-0.004425)*BuyPrice-(1+0.001425)*SellPrice))
Date_List = unique(strategy_Data$Date)



strategy_df = data.frame()
for(i in 1:length(Date_List)){
  temp = strategy_Data %>% filter(Date == Date_List[i])
  Profit_Portfolio = sum(temp$Profit_de_trade)*1000
  df = data.frame(Date = Date_List[i], profit = Profit_Portfolio)
  strategy_df = strategy_df %>% rbind(df)
}
strategy_df$Date = ymd(strategy_df$Date)
strategy_df = strategy_df %>%
  arrange(Date)%>%
  mutate(cumProfit = cumsum(profit))
ggplot(data = strategy_df ,aes(x= Date+42,y = cumProfit))+
  geom_line()+
  labs(x = 'Date',y='Strategy Profit')+ggtitle('Momentum Strategy') +
  theme(plot.background = element_rect(fill = "#BFD5E3"))
#====================Result==============================#
Win_Ret = length(which(strategy_df$profit>0))/nrow(strategy_df)
MDD =mdd(strategy_df$cumProfit)*1000
ContLossWeeks = max_Cont(strategy_df$cumProfit,stream = 'Loss')
ContWinWeeks = max_Cont(strategy_df$cumProfit,stream = 'Win')
Profit_fac = Profit_factor(strategy_df$profit)
Avg_Profit_Loss = strategy_df %>%
  mutate(WinLoss = ifelse(profit>=0,'+','-')) %>%
  group_by(WinLoss) %>% summarise(Avg_Scale = mean(profit))

tp = strategy_Data %>% filter(Date == '2018-01-05')
Initial_cap = sum(tp$BuyPrice[which(tp$GH_group == 5)])*1000*1.001425-sum(tp$BuyPrice[which(tp$GH_group == 1)])*1000*(1-0.004425)











