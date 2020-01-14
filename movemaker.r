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


# Stock Detail from Miniload
###############################################################################################################
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



# EXPECTED DEMAND
##########################################################################################################

# Confirmed Sales - likelihood has been set to 1
confirmed_sales <- tbl(simcon, sql("select * from confirmed_sales")) %>%
  collect() %>%
  mutate(LIKELIHOOD=1.0) %>%
  select(PROD_ID, LIKELIHOOD, EXP_DEMAND=DEMAND) %>%
  setDT(.)


# Forecasted Demand - Based on Average Daily Sales

forecast_demand <- tbl(simcon, sql("select * from average_daily_sales where order_type = 'SHOP'")) %>%
  collect() %>%
  mutate(EXP_DEMAND=ceiling(EXP_DEMAND)) %>%
  select(PROD_ID, LIKELIHOOD, EXP_DEMAND) %>%
  filter(LIKELIHOOD > 0.95) %>% # specify acceptable minimum likelihood
  setDT(.)


# products with expected sales, but not captured in confirmed sales
surplus_demand <- data.table(data.frame(PROD_ID = setdiff(forecast_demand$PROD_ID, confirmed_sales$PROD_ID)))

setkey(forecast_demand, PROD_ID)
setkey(surplus_demand, PROD_ID)


additional_sales <- forecast_demand[surplus_demand]
total_expected_demand <- rbind(confirmed_sales, additional_sales)

###############################################################################################################

# Stocked Products with Demand

asrs_with_demand <- merge(stock[LOCATION == 'ASRS'], total_expected_demand, by.x = "ITEM_NUMBER", by.y = "PROD_ID")[, TOTAL_STOCK := sum(QUANTITY), by=ITEM_NUMBER]
osr_with_demand <- merge(stock[LOCATION == 'OSR'], total_expected_demand, by.x = "ITEM_NUMBER", by.y = "PROD_ID")[, TOTAL_STOCK := sum(QUANTITY), by=ITEM_NUMBER]


# Begin with products in the OSR
##############################################################################################################

# No demand expected for these products - Send to ASRS where currently stocked in OSR
to_asrs1 <- stock %>%
  left_join(total_expected_demand, by = c("ITEM_NUMBER" = "PROD_ID")) %>%
  filter(LOCATION == 'OSR' & is.na(EXP_DEMAND)) %>%
  select(ITEM_NUMBER, LOCATION, LOCNO, LICENCEPLATE, QUANTITY) %>%
  setDT(.)

# Pproducts with more than required minimum to fulfill expected demand
to_asrs2 <- osr_with_demand %>%
  mutate(CLASSIFICATION = if_else(EXP_DEMAND <= CUMUNITS, 1, 0)) %>%
  filter(TOTAL_STOCK >= EXP_DEMAND) %>%
  group_by(ITEM_NUMBER, CLASSIFICATION) %>%
  mutate(RNK = 1:n()) %>%
  filter(CLASSIFICATION == 1 & RNK > 1) %>%
  ungroup() %>%
  select(ITEM_NUMBER, LOCATION, LOCNO, LICENCEPLATE, QUANTITY) %>%
  setDT(.)

to_asrs <- rbind(to_asrs1, to_asrs2)



## Determine Shortfall in OSR
osr_shortfall <- osr_with_demand %>%
  mutate(CLASSIFICATION = if_else(EXP_DEMAND <= CUMUNITS, 1, 0)) %>%
  filter(TOTAL_STOCK < EXP_DEMAND) %>%
  group_by(ITEM_NUMBER) %>%
  summarise(REQUIRED = min(EXP_DEMAND - TOTAL_STOCK)) %>%
  setDT(.)


# From ASRS to OSR to cover shortfall
to_osr1 <- asrs_with_demand %>%
  inner_join(osr_shortfall, by="ITEM_NUMBER") %>%
  mutate(CLASSIFICATION = if_else(REQUIRED <= CUMUNITS, 1, 0)) %>%
  group_by(ITEM_NUMBER, CLASSIFICATION) %>%
  mutate(RNK = 1:n()) %>%
  filter(CLASSIFICATION == 0 | CLASSIFICATION == 1 & RNK == 1) %>%
  ungroup() %>%
  select(ITEM_NUMBER, LOCATION, LOCNO, LICENCEPLATE, QUANTITY) %>%
  setDT(.)

# Products in ASRS not in OSR, with expected sales
to_osr2 <- asrs_with_demand %>%
  inner_join(data.frame(ITEM_NUMBER = setdiff(asrs_with_demand$ITEM_NUMBER, osr_with_demand$ITEM_NUMBER), stringsAsFactors = F), by="ITEM_NUMBER") %>%
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