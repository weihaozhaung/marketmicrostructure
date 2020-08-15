rm(list = ls());gc()
library(lubridate)
library(tidyverse)
library(rlist)
library(foreach)
library(stringr)
library(doParallel)
library(data.table)
library(doSNOW)
library(tibbletime)
library(zoo)
setwd("C:/Users/Eric/Desktop/FinanceManagement/MicroMarket/HW5/Data")

Files = list.files()
#------Func--------#
sum_50 = rollify(sum, window = 50)
#=========VPIN==========#
cl = makeCluster(7)
registerDoSNOW(cl)
pb <- txtProgressBar(max = length(Files), style = 3)
progress <- function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)

VPIN_RESULT = foreach(iy = 1:length(Files),
                      .packages = c('dplyr','data.table','lubridate','tibbletime','zoo'),
                      .combine = 'rbind',
                      .options.snow = opts)%dopar%{
ItrDy_data = fread(Files[iy],data.table = F)[,-1]

ItrDy_data_format = ItrDy_data %>%
  filter(BuySell == 'S',MthTime <= 13300000, MthDate>=20171101) %>%
  group_by(MthDate,MthTime) %>%
  summarise(StkNo = StkNo[1], PRC = MthPr[1], VOL = sum(MthShr)) %>% 
  ungroup() %>%
  mutate(delta_P = PRC-lag(PRC))

#====================VPIN======================#
#-----Initial--------#
daily_avg_vol = ItrDy_data_format %>%
  group_by(MthDate) %>%
  summarise(Daily_vol = sum(VOL)) %>% 
  ungroup() %>% 
  summarise(vol = mean(Daily_vol)) %>%
  as.numeric()
sd_P = ItrDy_data_format %>%
  summarise(sd_P = sd(delta_P,na.rm= T)) %>% as.numeric()

Bucket_vol = daily_avg_vol/50
#--------------------#
VPIN_table = ItrDy_data_format %>%
  na.omit() %>% 
  mutate(cum_vol = cumsum(VOL)) %>% 
  mutate(Bucket = cum_vol%/%Bucket_vol+1, Excess_vol = (cum_vol/Bucket_vol-cum_vol%/%Bucket_vol)*Bucket_vol) %>% 
  as.data.frame()
VPIN_table_temp = data.frame()
for(i in 1:(length(unique(VPIN_table$Bucket))-1)){
  temp = VPIN_table %>% filter(Bucket == unique(VPIN_table$Bucket)[i])
  if(i!=1&nrow(temp)!=1){temp1 = temp[-1,]}
  append1 = VPIN_table %>% filter(Bucket == unique(VPIN_table$Bucket)[i+1]) %>% slice(1)
  append2 = append1
  append1$Bucket[1] = unique(VPIN_table$Bucket)[i]
  append1$VOL[1] = Bucket_vol-temp$Excess_vol[nrow(temp)]
  append2$VOL[1] = (append2$VOL[1]%%Bucket_vol)- append1$VOL[1]
  VPIN_table_temp = VPIN_table_temp  %>% rbind(append1) %>%rbind(append2)
  if(i==1){temp1=temp}
  if(nrow(temp)!=1){VPIN_table_temp = VPIN_table_temp %>% rbind(temp1)}
}
VPIN_table = VPIN_table_temp %>% 
  select(-Excess_vol) %>% 
  filter(Bucket != max(Bucket)) %>% 
  mutate(Buy_VOL = pnorm(delta_P/sd_P)*VOL,Sell_VOL = (1-pnorm(delta_P/sd_P))*VOL) %>% 
  group_by(Bucket) %>% 
  summarise(MthDate = MthDate[1], MthTime = MthTime[n()],
            StkNo = StkNo[1], VB = sum(Buy_VOL),
            VS = sum(Sell_VOL), VT = sum(VOL))
join_tbl = data.frame(Bucket =VPIN_table$Bucket[1]:VPIN_table$Bucket[nrow(VPIN_table)])
VPIN_table = join_tbl %>% left_join(VPIN_table, by = 'Bucket')
VPIN_table$MthDate = na.locf(VPIN_table$MthDate)
VPIN_table$MthTime = na.locf(VPIN_table$MthTime)
VPIN_table$StkNo = na.locf(VPIN_table$StkNo)
VPIN_table$VT[which(is.na(VPIN_table$VT))] = Bucket_vol
VPIN_table[is.na(VPIN_table)]=0

VPIN_output = VPIN_table %>% mutate(VPIN = sum_50(abs(VS-VB))/(50*Bucket_vol))
return(VPIN_output)
                      }
stopCluster(cl)
stopImplicitCluster()
setwd("C:/Users/Eric/Desktop/FinanceManagement/MicroMarket/HW5")
write.csv(VPIN_RESULT,'VPIN_RESULT.csv',row.names = F)


#=================================================DPIN=============================================#
setwd("C:/Users/Eric/Desktop/FinanceManagement/MicroMarket/HW5/Data")
cl = makeCluster(7)
registerDoSNOW(cl)
pb <- txtProgressBar(max = length(Files), style = 3)
progress <- function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)

DPIN_RESULT = foreach(ia = 1:length(Files),
                      .packages = c('dplyr','data.table','lubridate','tibbletime','zoo'),
                      .combine = 'rbind',
                      .options.snow = opts)%dopar%{
ItrDy_data = fread(Files[ia],data.table = F)[,-1]

ItrDy_data_format = ItrDy_data %>%
  filter(BuySell == 'S',MthTime <= 13300000, MthDate>=20171101) %>%
  group_by(MthDate,MthTime) %>%
  summarise(StkNo = StkNo[1], PRC = MthPr[1], VOL = sum(MthShr)) %>% 
  group_by(MthDate) %>%
  mutate(delta_P = PRC - last(PRC)) %>%
  ungroup() %>%
  mutate(sd_P = sd(delta_P)) %>% 
  mutate(Buy_VOL = pnorm(delta_P/sd_P)*VOL,Sell_VOL = (1-pnorm(delta_P/sd_P))*VOL) %>% 
  mutate(itvl = (((MthTime)%/%1000000-9)*60+(MthTime%%1000000)%/%10000)%/%15+1) %>% 
  mutate(MthDate = ymd(MthDate)) %>%
  mutate(weekday = weekdays(MthDate))


#----------------------------#
ItrDy_data_format$itvl[which(ItrDy_data_format$itvl==19)] = 18
#----------------------------#
DPIN_table = ItrDy_data_format %>%
  group_by(MthDate,itvl) %>%
  summarise(StkNo = StkNo[1], PRC = last(PRC),
            weekday = last(weekday), VB_ratio = sum(Buy_VOL)/sum(VOL),
            VS_ratio = sum(Sell_VOL)/sum(VOL)) %>% 
  group_by(MthDate) %>% mutate(log_ret = log(PRC)-log(lag(PRC))) %>% 
  ungroup() %>% na.omit()
DailyRet = DPIN_table %>% group_by(MthDate) %>% summarise(dailyRet = sum(log_ret, na.rm = T)) %>% ungroup()
join_df = data.frame()
for(is in 1:length(unique(DPIN_table$MthDate))){
  st = DPIN_table %>% filter(MthDate == unique(DPIN_table$MthDate)[is])
  temp = data.frame(MthDate = unique(DPIN_table$MthDate)[is],itvl = seq(st$itvl[1],st$itvl[length(st$itvl)]))
  join_df = join_df %>% rbind(temp)
}
DPIN_table = join_df %>% left_join(DPIN_table, by = c('MthDate', 'itvl'))
for(i in 3:5){
  eval(parse(text = paste0("DPIN_table$",
                           colnames(DPIN_table)[i],
                           " = na.locf(DPIN_table$",
                           colnames(DPIN_table)[i],")")))
}
for(i in 6:8){
  eval(parse(text = paste0("DPIN_table$",
                           colnames(DPIN_table)[i],
                           "[which(is.na(DPIN_table$",
                           colnames(DPIN_table)[i],"))]= 0")))
}
for(ix in 1:12){
  eval(parse(text = paste0("DailyRet = DailyRet %>% mutate(d",
                           ix,"_ret= lag(dailyRet,",ix,"))")))
}

DPIN_table_na = DPIN_table %>% left_join(DailyRet, by ='MthDate') %>% na.omit()
#-------------#
DPIN_table_na$itvl = as.factor(DPIN_table_na$itvl)
DPIN_table_na$weekday = as.factor(DPIN_table_na$weekday)
#-------------#
regressor = lm(data = DPIN_table_na, formula = log_ret ~ weekday + itvl + d1_ret +
                  d2_ret + d3_ret + d4_ret + d5_ret + d6_ret + d7_ret + d8_ret + d9_ret + d10_ret+ d11_ret+ d12_ret)

DPIN_table_na = DPIN_table_na %>% mutate(res = regressor$residuals)

DPIN_output = DPIN_table_na %>%
  select(MthDate, itvl, StkNo, VB_ratio, VS_ratio, res) %>%
  mutate(DPIN = ifelse(res>=0, VS_ratio, VB_ratio))
return(DPIN_output)
}
stopCluster(cl)
stopImplicitCluster()
setwd("C:/Users/Eric/Desktop/FinanceManagement/MicroMarket/HW5")
write.csv(DPIN_RESULT,'DPIN_RESULT.csv',row.names = F)








