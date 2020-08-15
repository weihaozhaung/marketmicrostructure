#rm(list=ls());gc()

library(rvest)
library(stringr)
setwd("C:/Users/Eric/Desktop/FinanceManagement/MicroMarket/crawl_Data")

stock_list=c(1201,1203,1210,1215,1216,1217,
             1218,1219,1220,1225,1227,1229) %>%as.character()

#一樣所以可以不用改
stock_subset1=c(1201,1203,1210,1215,1216,1217,
             1218,1219,1220,1225,1227,1229) %>%as.character()
# stock_list_2=c()%>%as.character()
for(ix in 1:270){
  
for(iy in stock_list){
  if(iy%in%stock_subset1){y=5}
# else if(iy%in%stock_subset2){y=6}

html_txt <- read_html(paste0("https://tw.stock.yahoo.com/q/ts?s=",iy,"&t=50"),encoding = "big5")
tag_table <- html_nodes(html_txt,"td")
stock_data=data.frame()
 
for (i in c(1:50)){

  time = tag_table[[i*6 + 10+y]] %>% html_text() %>% as.character()
  if(time=="14:30:00"){next}
  PB = tag_table[[i*6 +11+y]] %>% html_text() %>% as.numeric()
  if(is.na(as.numeric(PB))){break}
  PS = tag_table[[i*6 + 12+y]] %>% html_text()%>% as.numeric()
  P = tag_table[[i*6 + 13+y]] %>% html_text()%>% as.numeric()
   HL = tag_table[[i*6 + 14+y]] %>% html_text() %>% as.character() %>% substring(2,10)
  VOL = tag_table[[i*6 + 15+y]] %>% html_text()%>% as.numeric()


  stock_data = stock_data %>%
    rbind(data.frame(time = time ,
                    buyPrice = PB,
                    soldprice = PS,
                    P=P,
                    HL=HL,
                    VOL=VOL))
}
write.csv(stock_data,paste0("Stock_",iy,"_",
                            Sys.time() %>% substring(1,10),"_",
                            Sys.time() %>% substring(12,13),
                            Sys.time() %>% substring(15,16),
                            Sys.time() %>% substring(18,19),
                            ".csv"))
}
  # if(ix==270){break}
  # for(iz in 1:60){
  # print(paste0("Please wait :",as.character(60-iz)," secs"))
  #   Sys.sleep(1)
  #   }
  # 
  # print(paste0(Sys.time(),"......","OK!"))
  
}
devtools::install_github("PMassicotte/gtrendsR")
#library(gtrendsR)
gtrendsR::gtrends(keyword = 'cat', geo = "US", time = "2010-01-01 2010-04-03",gprop = 'web',onlyInterest = TRUE)
gtrendsR::gtrends(keyword = "Superbowl", geo = "US", time = "2017-12-10 2018-01-20",onlyInterest = TRUE)
plot(gtrendsR::gtrends(keyword = "Superbowl", geo = "US", time = "2017-12-10 2018-01-20"))
