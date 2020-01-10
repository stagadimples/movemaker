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
livecon <- dbConnect(odbc::odbc(), dsn = "live", pwd = "atis0815")
simcon <- dbConnect(odbc::odbc(), dsn = "simulation", pwd = "sim123")
##########################################################################################################

stock <- tbl(livecon, sql(
"
select location
  , locno
  , z_pos
  , loccount
  , licenceplate
  , item_number
  , quantity
  , sum(quantity) over (partition by item_number, location order by quantity desc, loccount desc, locno, z_pos, licenceplate)cumunits
  , row_number() over (partition by item_number, location order by quantity desc, loccount desc, locno, z_pos, licenceplate)itemrank
from
(
  select location
    , licenceplate
    , item_number
    , quantity
    , count(*) over (partition by item_number, locno, location order by locno)loccount
    , locno
    , z_pos
  from
  (
   select case
      when substr(l.locno, 1, 3) ='OSM' then 'OSR'
      when substr(l.locno, 1, 3) = 'ASM' then 'ASRS'
    end location
    , c.licenceplate
    , i.item_number
    , st.act_quantity quantity
    , l.locno
    , c.z_pos
   from stock@motion st
    inner join cont@motion c on st.cont_id = c.id
    inner join item@motion i on st.item_id = i.id
    inner join loc@motion l on c.loc_id = l.id
    inner join tm_locs@motion tl on c.loc_id = tl.loc_id
    inner join states@motion s on tl.state_id = s.id
   where c.cont_type_id != 0
    and substr(l.locno, 1, 3) in ('OSM', 'ASM')
    and s.domain_id = 2
  )
)
"
)) %>%
  collect() %>%
  setDT(.)


daily_sales <- tbl(livecon, sql(
"
select * from average_daily_sales@k1233simu
where order_type = 'SHOP'
"
)) %>%
  collect() %>%
  mutate(EXP_DEMAND=ceiling(EXP_DEMAND)) %>%
  select(PROD_ID, LIKELIHOOD, EXP_DEMAND) %>%
  filter(LIKELIHOOD > 0.5) %>% # specify acceptable minimum likelihood
  setDT(.)


# Demand data that is almost guaranteed to be accurate - likelihood has been set to 1
confirmed_sales <- tbl(livecon, sql(
"
 select prod_id
  , sum(demand)demand
 from
 (
   select p.system_product_id prod_id
    , sum(lc.requested_quantity)demand
   from originator_headers orh
    inner join originator_lines orl on orh.id = orl.originator_header_id
    inner join logical_components lc on orl.id = lc.originator_line_id
    inner join products p on orl.item_id = p.id
   where lc.dtype = 'KiDropPointLineK1233'
    and lc.state = 4
   group by p.system_product_id
   
   union all
   
   select prod_id
    , unit_qty
   from d2c_next_day_demand
   where trunc(input_date) = trunc(sysdate) - 1
 )
 group by prod_id
"
)) %>%
  collect() %>%
  mutate(LIKELIHOOD=1.0) %>%
  select(PROD_ID, LIKELIHOOD, EXP_DEMAND=DEMAND) %>%
  setDT(.)


# products with expected sales, but not captured in confirmed sales
daily_sales_only <- data.table(data.frame(PROD_ID = setdiff(daily_sales$PROD_ID, confirmed_sales$PROD_ID)))

setkey(daily_sales, PROD_ID)
setkey(daily_sales_only, PROD_ID)


additional_sales <- daily_sales[daily_sales_only]

total_expected_demand <- rbind(confirmed_sales, additional_sales)

###############################################################################################################

asrs <- merge(stock[LOCATION == 'ASRS'], total_expected_demand, by.x = "ITEM_NUMBER", by.y = "PROD_ID")[, TOTAL_STOCK := sum(QUANTITY), by=ITEM_NUMBER]
osr <- merge(stock[LOCATION == 'OSR'], total_expected_demand, by.x = "ITEM_NUMBER", by.y = "PROD_ID")[, TOTAL_STOCK := sum(QUANTITY), by=ITEM_NUMBER]


# Begin with products in the OSR
##############################################################################################################

to_asrs <- osr %>%
  mutate(CLASSIFICATION = if_else(EXP_DEMAND <= CUMUNITS, 1, 0)) %>%
  filter(TOTAL_STOCK >= EXP_DEMAND) %>%
  group_by(ITEM_NUMBER, CLASSIFICATION) %>%
  mutate(RNK = 1:n()) %>%
  filter(CLASSIFICATION == 1 & RNK > 1) %>%
  ungroup() %>%
  select(ITEM_NUMBER, LOCATION, LOCNO, LICENCEPLATE, QUANTITY) %>%
  setDT(.)

## Determine Shortfall in OSR
osr_shortfall <- osr %>%
  mutate(CLASSIFICATION = if_else(EXP_DEMAND <= CUMUNITS, 1, 0)) %>%
  filter(TOTAL_STOCK < EXP_DEMAND) %>%
  group_by(ITEM_NUMBER) %>%
  summarise(REQUIRED = min(EXP_DEMAND - TOTAL_STOCK)) %>%
  setDT(.)


# From ASRS to OSR to cover shortfall
to_osr1 <- asrs %>%
  inner_join(osr_shortfall, by="ITEM_NUMBER") %>%
  mutate(CLASSIFICATION = if_else(REQUIRED <= CUMUNITS, 1, 0)) %>%
  group_by(ITEM_NUMBER, CLASSIFICATION) %>%
  mutate(RNK = 1:n()) %>%
  filter(CLASSIFICATION == 0 | CLASSIFICATION == 1 & RNK == 1) %>%
  ungroup() %>%
  select(ITEM_NUMBER, LOCATION, LOCNO, LICENCEPLATE, QUANTITY) %>%
  setDT(.)

# Products in ASRS not in OSR, with expected sales
to_osr2 <- asrs %>%
  inner_join(data.frame(ITEM_NUMBER = setdiff(asrs$ITEM_NUMBER, osr$ITEM_NUMBER), stringsAsFactors = F), by="ITEM_NUMBER") %>%
  mutate(CLASSIFICATION = if_else(EXP_DEMAND <= CUMUNITS, 1, 0)) %>%
  group_by(ITEM_NUMBER, CLASSIFICATION) %>%
  mutate(RNK = 1:n()) %>%
  filter(CLASSIFICATION == 0 | CLASSIFICATION == 1 & RNK == 1) %>%
  ungroup() %>%
  select(ITEM_NUMBER, LOCATION, LOCNO, LICENCEPLATE, QUANTITY) %>%
  setDT(.)

to_osr <- to_osr1 %>%
  rbind(to_osr2)


# Prescribed moves based on expected shop demand for the day
# This table may now be used to populate imp_cont_info_man in motion. The column names have been carefully chosen to allign the 2 tables 
moves <- rbind(cbind(TARGET_STATE_NAME = "Z-MOVE_ASRS", to_asrs), cbind(TARGET_STATE_NAME = "Z-MOVE_OSR", rbind(to_osr1, to_osr2)))[
  , .(LICENCEPLATE, TARGET_STATE_NAME, SOURCE_ZONE_NAME="Z-ABIN2BIN", TIMESTAMP = as.character(Sys.time()), STATUS = 90)]


# Write moves into a table in Simulation Server
dbWriteTable(simcon, 
               "TEST_MINILOAD_REORG", 
               value=moves,
               overwrite = TRUE,
               field.types = c(
                 LICENCEPLATE = "varchar2(50)",
                 TARGET_STATE_NAME = "varchar2(50)",
                 SOURCE_ZONE_NAME = "varchar2(50)",
                 TIMESTAMP = "date",
                 STATUS = "number"
               )
             )


# Disconnect from databases
dbDisconnect(livecon); dbDisconnect(simcon)