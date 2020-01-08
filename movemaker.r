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
        when substr(locno, 1, 3) ='OSM' then 'OSR'
        when substr(locno, 1, 3) = 'ASM' then 'ASRS'
      end location
      , licenceplate
      , item_number
      , quantity
      , locno
      , z_pos
    from v_afm_stock@motion
    where substr(locno, 1, 3) in ('OSM', 'ASM')
  )
)
"
)) %>%
  collect() %>%
  setDT(.)


average_sales <- tbl(livecon, sql("select * from average_daily_sales@k1233simu")) %>%
  collect() %>%
  setDT(.)

dbDisconnect(livecon)


###############################################################################################################

asrs <- merge(stock[LOCATION == 'ASRS'], average_sales, by.x = "ITEM_NUMBER", by.y = "PROD_ID")[, TOTAL_STOCK := sum(QUANTITY), by=ITEM_NUMBER]
osr <- merge(stock[LOCATION == 'OSR'], average_sales, by.x = "ITEM_NUMBER", by.y = "PROD_ID")[, TOTAL_STOCK := sum(QUANTITY), by=ITEM_NUMBER]



# Begin with products in the OSR
##############################################################################################################

to_asrs <- osr %>%
  mutate(LIKELIHOOD = DAYS_ORDERED / 30,
         EXP_UNITS = round(LIKELIHOOD * AVG_UNITS),
         EXP_ORDERS = round(LIKELIHOOD * AVG_ORDERS),
         CLASSIFICATION = if_else(EXP_UNITS <= CUMUNITS, 1, 0)
         ) %>%
  filter(LIKELIHOOD >= 0.5) %>%
  filter(TOTAL_STOCK >= EXP_UNITS) %>%
  group_by(ITEM_NUMBER, CLASSIFICATION) %>%
  mutate(RNK = 1:n()) %>%
  filter(CLASSIFICATION == 1 & RNK > 1) %>%
  ungroup() %>%
  select(ITEM_NUMBER, LOCATION, LOCNO, LICENCEPLATE, QUANTITY) %>%
  setDT(.)


## Determine Shortfall in OSR

osr_shortfall <- osr %>%
  mutate(LIKELIHOOD = DAYS_ORDERED / 30,
         EXP_UNITS = round(LIKELIHOOD * AVG_UNITS),
         EXP_ORDERS = round(LIKELIHOOD * AVG_ORDERS),
         CLASSIFICATION = if_else(EXP_UNITS <= CUMUNITS, 1, 0)
  ) %>%
  filter(LIKELIHOOD >= 0.5) %>%
  filter(TOTAL_STOCK < EXP_UNITS) %>%
  group_by(ITEM_NUMBER) %>%
  summarise(REQUIRED = min(EXP_UNITS - TOTAL_STOCK)) %>%
  setDT(.)



to_osr1 <- asrs %>%
  inner_join(osr_shortfall, by="ITEM_NUMBER") %>%
  mutate(LIKELIHOOD = DAYS_ORDERED / 30,
         EXP_UNITS = round(LIKELIHOOD * AVG_UNITS),
         EXP_ORDERS = round(LIKELIHOOD * AVG_ORDERS),
         CLASSIFICATION = if_else(REQUIRED <= CUMUNITS, 1, 0)
  ) %>%
  filter(LIKELIHOOD >= 0.5) %>%
  group_by(ITEM_NUMBER, CLASSIFICATION) %>%
  mutate(RNK = 1:n()) %>%
  filter(CLASSIFICATION == 0 | CLASSIFICATION == 1 & RNK == 1) %>%
  ungroup() %>%
  select(ITEM_NUMBER, LOCATION, LOCNO, LICENCEPLATE, QUANTITY) %>%
  setDT(.)



# Products in ASRS not in OSR, with expected sales

to_osr2 <- asrs %>%
  inner_join(data.frame(ITEM_NUMBER = setdiff(asrs$ITEM_NUMBER, osr$ITEM_NUMBER), stringsAsFactors = F), by="ITEM_NUMBER") %>%
  mutate(LIKELIHOOD = DAYS_ORDERED / 30,
         EXP_UNITS = round(LIKELIHOOD * AVG_UNITS),
         EXP_ORDERS = round(LIKELIHOOD * AVG_ORDERS),
         CLASSIFICATION = if_else(EXP_UNITS <= CUMUNITS, 1, 0)
  ) %>%
  filter(LIKELIHOOD >= 0.5) %>%
  group_by(ITEM_NUMBER, CLASSIFICATION) %>%
  mutate(RNK = 1:n()) %>%
  filter(CLASSIFICATION == 0 | CLASSIFICATION == 1 & RNK == 1) %>%
  ungroup() %>%
  select(ITEM_NUMBER, LOCATION, LOCNO, LICENCEPLATE, QUANTITY) %>%
  setDT(.)


to_osr <- to_osr1 %>%
  rbind(to_osr2)


moves <- rbind(cbind(DIRECTION = "TO_ASRS", to_asrs), cbind(DIRECTION = "TO_OSR", rbind(to_osr1, to_osr2)))
