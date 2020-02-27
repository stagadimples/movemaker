##########################################################################################################

library(odbc)
library(tidyverse)
library(dbplyr)
library(lubridate)
library(data.table)
library(caret)

# Garbage Collection
################################################################################
rm(list = ls())
gc()
################################################################################

##########################################################################################################
simcon <- dbConnect(odbc::odbc(), dsn = "simulation", pwd = "sim123")
livecon <- dbConnect(odbc::odbc(), dsn = "live", pwd = "atis0815")
##########################################################################################################


# Stock Detail from Miniload
###############################################################################################################

stock_sql <-
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
     from stock@mcs_live st
      inner join cont@mcs_live c on st.cont_id = c.id
      inner join item@mcs_live i on st.item_id = i.id
      inner join loc@mcs_live l on c.loc_id = l.id
      inner join tm_locs@mcs_live tl on c.loc_id = tl.loc_id
      inner join states@mcs_live s on tl.state_id = s.id
     where c.cont_type_id != 0
      and substr(l.locno, 1, 3) in ('OSM', 'ASM')
      and s.domain_id = 2
    )
  )
"

stock <- tbl(simcon, sql(stock_sql)) %>%
  collect() %>%
  setDT(.)



# Get sale price information from host import event message
sql_orh <- 
  "
  select raw_message 
  from host_imp_event 
  where telegram_type = 'ORH'
    and trunc(creation_time) = trunc(sysdate) - 1
"

# full structure of messages

orh_ptr <- tbl(livecon, sql(sql_orh))

orh <- orh_ptr %>%
  collect() %>%
  select(RAW_MESSAGE)

messageList <- strsplit(orh$RAW_MESSAGE, split = "\\|")
messageList <- unlist(messageList)

# Get separate Lists for ORH and ORL
orlList <- messageList[substr(messageList, 39, 41) == "ORL"]

# Split ORL based on specification
orl_func <- function(x){
  substring(x, 
            c(1, 39, 45, 53, 59, 63, 65, 85, 90, 98, 104, 110, 111, 161, 167, 171), 
            c(38, 44, 52, 58, 62, 64, 84, 89, 97, 103, 109, 110, 160, 166, 170, 180)
  )
}

orl_labels <- c("seqno", "recordid", "date", "time", "dc", "hostid", 
                "orderid", "orderline", "prodid", "max_qty", "min_qty", "clearind", "glmessage", "refno1", "refno2", "saleprice")

orl_table_temp <- lapply(orlList, orl_func)
orl_table <- as.data.frame(do.call("rbind", orl_table_temp), stringsAsFactors=F)
names(orl_table) <- orl_labels



# Refine Outputs
refineOutput <- function(x){
  trimws(x)
  str_replace(x, "@", "")
}

orl_table[] <- lapply(orl_table[], refineOutput)

orl_table[c("dc", "orderline", "max_qty", "min_qty", "saleprice")] <- lapply(orl_table[c("dc", "orderline", "max_qty", "min_qty", "saleprice")], as.numeric)


prices <- orl_table %>%
  filter(complete.cases(.)) %>%
  mutate(saleprice = saleprice / 100) %>%
  group_by(prodid) %>%
  summarise(saleprice = min(saleprice)) %>%
  select(prodid, saleprice) %>%
  unique(.)



# Prepare forecast

# Training Set
demand_train <- tbl(simcon, sql("select * from shop_demand_training_data")) %>%
  collect() %>%
  sample_n(nrow(.)) %>%
  left_join(prices, by = c("PROD_ID" = "prodid")) %>%
  select(-c(PROD_ID, SALE_CLASS, RNK)) %>%
  mutate(DIRECTORATE = factor(DIRECTORATE),
         PARTNER_DISCOUNT = factor(PARTNER_DISCOUNT),
         HANGING_GARMENT = factor(HANGING_GARMENT),
         SOR_INDICATOR = factor(SOR_INDICATOR),
         SALEPRICE = if_else(saleprice == 0 | is.na(saleprice), SELLING_PRICE, saleprice),
         REDUCTION = SELLING_PRICE - SALEPRICE) %>%
  select(-saleprice, -SELLING_PRICE, -SOLD_REP_UNITS, -SALEPRICE, -REDUCTION, SELLING_PRICE, REDUCTION, SOLD_REP_UNITS) %>%
  setDT(.)


# Forecast
forecast_demand <- tbl(simcon, sql("select * from mv_movemaker_demand_features")) %>%
  collect() %>%
  sample_n(nrow(.)) %>%
  left_join(prices, by = c("PROD_ID" = "prodid")) %>%
  mutate(DIRECTORATE = factor(DIRECTORATE),
         PARTNER_DISCOUNT = factor(PARTNER_DISCOUNT),
         HANGING_GARMENT = factor(HANGING_GARMENT),
         SOR_INDICATOR = factor(SOR_INDICATOR),
         SALEPRICE = if_else(saleprice == 0 | is.na(saleprice), SELLING_PRICE, saleprice),
         REDUCTION = SELLING_PRICE - SALEPRICE) %>%
  select(-saleprice, -SELLING_PRICE, -SALEPRICE, -REDUCTION, SELLING_PRICE, REDUCTION) %>%
  setDT(.)


# K-Nearest Neighbours Model
mod_knn <- train(SOLD_REP_UNITS ~., data = subset(demand_train), method="knn",
                 preProcess = c("center", "scale"),
                 trControl=trainControl(method = "repeatedcv",
                                        repeats = 3))


# Update Forecast Base with Forecast Figures

forecast_demand$FORECAST <- round(predict(mod_knn, newdata = subset(forecast_demand, select = -PROD_ID))) %>%
  as.data.table(.)

forecast_demand <- forecast_demand[FORECAST > 0]


# EXPECTED DEMAND
##########################################################################################################

# Confirmed Sales - likelihood has been set to 1
confirmed_sales <- tbl(simcon, sql("select * from confirmed_sales")) %>%
  collect() %>%
  inner_join(data.frame(PROD_ID = unique(stock$ITEM_NUMBER), stringsAsFactors=F), by = "PROD_ID") %>%
  mutate(LIKELIHOOD=1.0) %>%
  select(PROD_ID, LIKELIHOOD, EXP_DEMAND=DEMAND) %>%
  setDT(.)


# products with expected sales, but not captured in co.nfirmed sales
surplus_demand <- data.table(data.frame(PROD_ID = setdiff(forecast_demand$PROD_ID, confirmed_sales$PROD_ID), stringsAsFactors = F))

setkey(forecast_demand, PROD_ID)
setkey(surplus_demand, PROD_ID)


additional_sales <- forecast_demand[surplus_demand, ]
additional_sales[, EXP_DEMAND := FORECAST]
total_expected_demand <- rbind(subset(confirmed_sales, select = c(PROD_ID, EXP_DEMAND)), subset(additional_sales, select = c(PROD_ID, EXP_DEMAND)))

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
  , .(LICENCEPLATE, TARGET_STATE_NAME, SOURCE_ZONE_NAME="Z-ABIN2BIN", TIMESTAMP = as.character(Sys.time()), STATUS = 10)]


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

# Disconnect from database
dbDisconnect(simcon); dbDisconnect(livecon)