rm(list = ls());gc()
library(lubridate)
library(tidyverse)
library(rlist)
library(foreach)
library(stringr)
library(doParallel)
library(data.table)
library(doSNOW)
library(tidyquant)
setwd("C:/Users/Eric/Desktop/FinanceManagement/MicroMarket/HW5/Data")
Deal_Files = list.files()
setwd("C:/Users/Eric/Desktop/FinanceManagement/MicroMarket/HW4/委託檔")
Files = list.files()
Files = Files[which(BookFiles%in%Deal_Files)]
get_mshw <- function(code = a){
  aaa <- a %>% 
    filter(OdrTime >= 9300000,
           OdrTime <= 13300000) %>% 
    mutate(h = as.numeric(ifelse(nchar(OdrTime)==7, substring(OdrTime,1,1),substring(OdrTime,1,2))),
           m = as.numeric(ifelse(nchar(OdrTime)==7, substring(OdrTime,2,3),substring(OdrTime,3,4)))) %>% 
    mutate(q = m %/% 15) %>% 
    filter(OdrShr > 0)
  sumMB_B <- aaa %>% 
    group_by(OdrDate, StkNo, h, q , Investor_Type, BuySell, OdrPr) %>%
    filter(BuySell == "B") %>% 
    summarise(sumMB = sum(OdrShr)) %>% 
    arrange(OdrDate, h, q, Investor_Type, desc(OdrPr)) %>% 
    mutate(n = 1:n()) %>% 
    filter(n == 1) %>% 
    group_by() %>% 
    select(OdrDate, StkNo, h, q, Investor_Type, sumMB)
  sumMB_S <- aaa %>% 
    group_by(OdrDate, StkNo, h, q , Investor_Type, BuySell, OdrPr) %>%
    filter(BuySell == "S") %>% 
    summarise(sumMB = sum(OdrShr)) %>% 
    arrange(OdrDate, h, q, Investor_Type, desc(OdrPr)) %>% 
    mutate(n = 1:n()) %>% 
    filter(n == n()) %>% 
    group_by() %>% 
    select(OdrDate, StkNo, h, q, Investor_Type, sumMB)
  MB_ratio_B <- aaa %>%
    group_by(OdrDate, StkNo, h, q, Investor_Type, BuySell) %>%
    filter(BuySell == "B") %>% 
    summarise(sumOB_type = sum(OdrShr)) %>% 
    left_join(sumMB_B, c("OdrDate", "StkNo","h", "q", "Investor_Type")) %>% 
    mutate(MB_Bratio = sumMB / sumOB_type) %>% 
    select(OdrDate, StkNo, h, q, Investor_Type, MB_Bratio)
  MB_ratio_S <- aaa %>%
    group_by(OdrDate, StkNo, h, q, Investor_Type, BuySell) %>%
    filter(BuySell == "S") %>% 
    summarise(sumOB_type = sum(OdrShr)) %>% 
    left_join(sumMB_S, c("OdrDate", "StkNo","h", "q", "Investor_Type")) %>% 
    mutate(MB_Sratio = sumMB / sumOB_type) %>% 
    select(OdrDate, StkNo, h, q, Investor_Type, MB_Sratio)
  meanOB <- aaa %>% 
    group_by(OdrDate, StkNo, h, q, Investor_Type) %>% 
    summarise(meanOB = mean(OdrShr))
  day_sumOB <- aaa %>% 
    group_by(OdrDate, Investor_Type) %>% 
    summarise(sumOB_d_type = sum(OdrShr))
  OB_ratio <- aaa %>% 
    group_by(OdrDate, StkNo, h, q, Investor_Type, BuySell) %>% 
    summarise(sumOB_type = sum(OdrShr)) %>% 
    spread(BuySell, sumOB_type) %>% 
    left_join(day_sumOB, by = c("OdrDate", "Investor_Type")) %>% 
    mutate(OB_Bratio = B / sumOB_d_type,
           OB_Sratio = S / sumOB_d_type) %>% 
    select(OdrDate, StkNo, h, q, Investor_Type, OB_Bratio, OB_Sratio)
  OIR <- aaa %>% 
    group_by(OdrDate, StkNo, h, q, Investor_Type, BuySell) %>% 
    summarise(BS_sumOB = sum(OdrShr)) %>% 
    spread(key = BuySell, value = BS_sumOB) %>% 
    mutate(B = ifelse(is.na(B), 0 , B),
           S = ifelse(is.na(S), 0 , S)) %>% 
    mutate(OIR = (B-S)/(B+S)) %>% 
    select(OdrDate, StkNo, h, q, Investor_Type, OIR)
  
  ans <- MB_ratio_B %>% 
    left_join(MB_ratio_S, by = c("OdrDate", "StkNo", "h", "q", "Investor_Type")) %>% 
    left_join(meanOB, by = c("OdrDate", "StkNo", "h", "q", "Investor_Type")) %>% 
    left_join(OB_ratio, by = c("OdrDate", "StkNo", "h", "q", "Investor_Type")) %>% 
    left_join(OIR, by = c("OdrDate", "StkNo", "h", "q", "Investor_Type"))
  return(ans)
}

cl = makeCluster(7)
registerDoSNOW(cl)
pb <- txtProgressBar(max = length(Files), style = 3)
progress <- function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)

mm_data = foreach( i = 1:length(Files),
                   .packages = c('dplyr','data.table','lubridate','tidyquant'),
                   .combine = 'rbind',
                   .options.snow = opts)%dopar%{
  a <- fread(Files[i], header = T, stringsAsFactors = F)
  z<- get_mshw(a)
  return(z)
                   }
mm_data$OdrDate = ymd(mm_data$OdrDate)

setwd("C:/Users/Eric/Desktop/MM_HW")
market = fread('market.csv',data.table = F)
colnames(market) = c('code','name','date','index')
market$date = ymd(market$date)
market= market %>% select(date,index) %>%  mutate(mkret = ifelse(index>=lag(index),'+','-'))
mm_data = mm_data %>% left_join(market,by = c('OdrDate'='date'))

individual = fread('individual.csv',data.table = F)
colnames(individual) = c('code','name','Yr','ind_ratio')

individual = individual %>% group_by(code) %>% slice(1) %>% ungroup()
individual$code = as.numeric(individual$code)
individual$ind_ratio = as.numeric(individual$ind_ratio)
mm_data = mm_data %>% left_join(individual,by = c('StkNo'='code'))
quantile(mm_data$ind_ratio,probs = 1/3,na.rm = T)
mm_data = mm_data %>%ungroup() %>% 
  mutate(ind = ifelse(ind_ratio<quantile(ind_ratio,probs = 1/3,na.rm = T),'Low_ind',
               ifelse(ind_ratio>quantile(ind_ratio,probs = 2/3,na.rm = T),'High_ind','Med_ind')))

setwd("C:/Users/Eric/Desktop/FinanceManagement/MicroMarket/HW5")
write.csv(mm_data,'OIB.csv',row.names = F)
