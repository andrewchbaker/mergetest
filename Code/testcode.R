library(data.table)

comp_test = data.table(readRDS(here::here("Data", "comp_test.rds")))
inst_test = data.table(readRDS(here::here("Data", "inst_test.rds")))

setkey(comp_test, cusip)
setkey(inst_test, cusip)

inst_data = merge(inst_test, comp_test, allow.cartesian = TRUE, by = "cusip")[
  rdate <= datadate & datadate - rdate <= months(11), .SD, by = cusip][
    , .SD[which.max(rdate)], by = .(cusip, fyear)]
