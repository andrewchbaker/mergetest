library(tidyverse)
library(dtplyr)
library(data.table)
library(pbapply)
library(parallel)

comp_test <- data.table(read_rds(here::here("Data", "comp_test.rds")))
inst_test <- data.table(read_rds(here::here("Data", "inst_test.rds")))

# function to find the closest filing within 6 months of the datadate
find_inst_match <- function(i) {
  inst_test %>% 
    .[cusip == comp_test$cusip[i] & 
        rdate <= comp_test$datadate[i] & 
        comp_test$datadate[i] - rdate <= months(11)] %>% 
    .[rdate == max(rdate), c("inst_shares")] %>% 
    .[, `:=`(firm_id = comp_test$firm_id[i], fyear = comp_test$fyear[i])] %>% 
    as_tibble()
}

# parallelize and run over all of our observations
# set cores
cl <- makeCluster(4)

## export data and functions to the clusters
clusterExport(cl, c("inst_test", "comp_test", "find_inst_match"))

# export needed programs
clusterEvalQ(cl, c(library(tidyverse), library(data.table)))

## run the command on the clusters
inst_data <- do.call(rbind, pblapply(cl = cl, X = 1:nrow(comp_short), FUN = find_inst_match))
stopCluster(cl)