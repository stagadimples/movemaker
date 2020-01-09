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
  filter(LIKELIHOOD >= 0.5) %>% # specify acceptable likelihood
  setDT(.)

dbDisconnect(livecon)


###############################################################################################################

asrs <- merge(stock[LOCATION == 'ASRS'], daily_sales, by.x = "ITEM_NUMBER", by.y = "PROD_ID")[, TOTAL_STOCK := sum(QUANTITY), by=ITEM_NUMBER]
osr <- merge(stock[LOCATION == 'OSR'], daily_sales, by.x = "ITEM_NUMBER", by.y = "PROD_ID")[, TOTAL_STOCK := sum(QUANTITY), by=ITEM_NUMBER]



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
moves <- rbind(cbind(TARGET = "ASRS", to_asrs), cbind(TARGET = "OSR", rbind(to_osr1, to_osr2)))[
  , .(LOCATION, TARGET, ITEM_NUMBER, LOCNO, LICENCEPLATE, QUANTITY)]
