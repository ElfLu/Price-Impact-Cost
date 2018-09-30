## Copyright (C) 2018 by Lu
##
## 目的：计算股票每一天的价格冲击成本，上海主板


#####----Guidance----#####
library(tidyverse)
library(cnquant)
library(parallel)


#####----计算价格冲击成本的并行程序----#####
#####----上海的路径----#####
main_path <- "D:/WIND_DATA/ID_BT_SHARES_A/Tick/SH"
compute_date <- dir(main_path)


#####----上海的并行运算----#####
cl <- makeCluster(detectCores())

clusterEvalQ(cl, library(tidyverse))
clusterEvalQ(cl, library(cnquant))

clusterExport(cl, "main_path")
clusterExport(cl, "compute_date")

SH_price_impact_cost_results <- parLapply(cl, compute_date[1:length(compute_date)], function(calculate_date){
  tryCatch({
    sub_path <- paste(main_path, calculate_date, sep = "/")
    file_name <- dir(sub_path)
    
    
    if (length(file_name) > 0) {
      file_path <- sapply(file_name, function(x){
        paste(sub_path, x, sep = "/")
      })
      
      res <- sapply(file_path, function(i){
        tick_data <- read_tick_data(i, 
                                    col_types = cols_only(code = col_integer(), time = col_integer(), 
                                                          price = col_integer(), ask10 = col_integer(), 
                                                          ask9 = col_integer(), ask8 = col_integer(), 
                                                          ask7 = col_integer(), ask6 = col_integer(), 
                                                          ask5 = col_integer(), ask4 = col_integer(), 
                                                          ask3 = col_integer(), ask2 = col_integer(), 
                                                          ask1 = col_integer(), bid1 = col_integer(), 
                                                          bid2 = col_integer(), bid3 = col_integer(), 
                                                          bid4 = col_integer(), bid5 = col_integer(), 
                                                          bid6 = col_integer(), bid7 = col_integer(), 
                                                          bid8 = col_integer(), bid9 = col_integer(), 
                                                          bid10 = col_integer(), 
                                                          asize10 = col_integer(), asize9 = col_integer(), 
                                                          asize8 = col_integer(), asize7 = col_integer(), 
                                                          asize6 = col_integer(), asize5 = col_integer(), 
                                                          asize4 = col_integer(), asize3 = col_integer(), 
                                                          asize2 = col_integer(), asize1 = col_integer(), 
                                                          bsize1 = col_integer(), bsize2 = col_integer(), 
                                                          bsize3 = col_integer(), bsize4 = col_integer(), 
                                                          bsize5 = col_integer(), bsize6 = col_integer(), 
                                                          bsize7 = col_integer(), bsize8 = col_integer(), 
                                                          bsize9 = col_integer(), bsize10 = col_integer()))
        
        price_impact_cost <- calculate_price_impact_cost(tick_data)
      })
      
      res <- as.data.frame(t(res))
      res <- res %>% na.omit()
      names(res) <- c("code", "price_impact_cost")
      res$date <- as.numeric(calculate_date)
      
      return(res)
    }
  }, error = function(e) return(c(NA, NA, NA)))
})

stopCluster(cl)

SH_price_impact_cost_results <- bind_rows(SH_price_impact_cost_results)






#####----Ending----#####
save.image("./01-D-SH-Price-Impact-Cost.RData")
