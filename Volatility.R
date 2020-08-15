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

#=========VPIN==========#
cl = makeCluster(7)
registerDoSNOW(cl)
pb <- txtProgressBar(max = length(Files), style = 3)
progress <- function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)

VOLATILITY = foreach(iy = 1:length(Files),
                      .packages = c('dplyr','data.table','lubridate','tibbletime','zoo'),
                      .combine = 'rbind',
                      .options.snow = opts)%dopar%{
ItrDy_data = fread(Files[iy],data.table = F)[,-1]
ItrDy_data = ItrDy_data %>%
  filter(BuySell == 'S') %>%
  group_by(MthDate, MthTime) %>% 
  summarise(StkNo = StkNo[1],PRC = mean(MthPr)) %>% 
  mutate(hr_min = MthTime%/%10000) %>% 
  ungroup() %>% group_by(MthDate, hr_min) %>% 
  summarise(StkNo = StkNo[1],PRC = last(PRC)) %>% 
  group_by(MthDate) %>% 
  mutate(Ret = log(PRC/lag(PRC)), lag_Ret = lag(Ret)) %>% 
  na.omit()
df = data.frame()
for(i in unique(ItrDy_data$MthDate)){
Reg = lm(data = ItrDy_data %>% filter(MthDate == i), formula = Ret ~ lag_Ret)
resid = Reg$residuals %>% as.numeric()
Realized_VOL = sum(resid**2)
temp = data.frame(StkNo = ItrDy_data$StkNo[1], MthDate = i, Re_VOL =Realized_VOL )
df = df %>% rbind(temp)
}
return(df)
                      }
stopCluster(cl)
stopImplicitCluster()

write.csv(VOLATILITY, 'Realized_Volatility.csv', row.names = F)

