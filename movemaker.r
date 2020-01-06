##########################################################################################################

library(odbc)
library(tidyverse)
library(dbplyr)
library(lubridate)
library(data.table)

# Garbage Collection
################################################################################
rm(list = ls())
gc()
################################################################################

##########################################################################################################
simcon <- dbConnect(odbc::odbc(), dsn = "simulation", pwd = "sim123")
##########################################################################################################

average_sales <- tbl(simcon, sql(
"
with base 
as
(
  select trunc(orh.creation_time)orderdate
    , p.system_product_id prod_id
    , p.product_no prod_code
    , p.item_desc description
    , sum(orl.requested_quantity)units
    , count(distinct orh.order_no)orders
  
  from (
    select * from originator_headers@lwms
    union
    select * from k1233kw_dw.originator_headers@lwms
  )orh
    inner join
  (
    select * from originator_lines@lwms
    union
    select * from k1233kw_dw.originator_lines@lwms
  )orl on orh.id = orl.originator_header_id
    inner join products@lwms p on orl.item_id = p.id
  where trunc(orh.creation_time) > trunc(sysdate) - 30
    and orh.order_type = 0
  group by trunc(orh.creation_time)
    , p.system_product_id
    , p.product_no
    , p.item_desc
)
select prod_id
  , prod_code
  , description
  , avg(units)avg_units
  , avg(orders)avg_orders
  , count(*)days_ordered
from base
group by prod_id
  , prod_code
  , description
"
)) %>%
  collect() %>%
  setDT(.)

dbDisconnect(simcon)

###############################################################################################################

average_sales[, LIKELIHOOD := DAYS_ORDERED /30][, ':='(EXP_UNITS = round(LIKELIHOOD * AVG_UNITS), EXP_ORDERS = round(LIKELIHOOD * AVG_ORDERS))]

