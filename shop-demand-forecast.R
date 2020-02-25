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


# base data for predictive model
###############################################################################################################
df <- tbl(simcon, sql("select * from shop_demand_training_data")) %>%
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


# Disconnect from databases
dbDisconnect(simcon); dbDisconnect(livecon)


intrain <- createDataPartition(df$HANGING_GARMENT, p=0.75, list = F)

dftrain <- df[intrain, ]
dftest <- df[-intrain, ]

mod_knn <- train(SOLD_REP_UNITS ~., data = dftrain, method="knn",
                 preProcess = c("center", "scale"),
                 trControl=trainControl(method = "cv"))

mod_svm <- train(SOLD_REP_UNITS ~., data = dftrain, method="svmRadial",
                 preProcess = c("center", "scale"),
                trControl=trainControl(method = "cv"))

mod_pls <- train(SOLD_REP_UNITS ~., data = dftrain, method = "pls",
                 preProcess = c("center", "scale"),
                 trControl = trainControl(method = "repeatedcv",
                                          repeats = 3))

mod_earth <- train(SOLD_REP_UNITS ~., data = dftrain, method = "earth",
                 preProcess = c("center", "scale"),
                 trControl = trainControl(method = "repeatedcv",
                                          repeats = 3))


# visual inspection of accuracy
svm_prediction <- predict(mod_svm, newdata = dftest)
knn_prediction <- predict(mod_knn, newdata = dftest)
pls_prediction <- predict(mod_pls, newdata = dftest)
earth_prediction <- predict(mod_earth, newdata = dftest)

dftest$svm <- round(abs(svm_prediction))
dftest$knn <- round(abs(knn_prediction))
dftest$pls <- round(abs(pls_prediction))
dftest$earth <- round(abs(earth_prediction))

# selected algorithm: partial least squares (least rmse)

RMSE(svm_prediction, dftest$SOLD_REP_UNITS)
RMSE(knn_prediction, dftest$SOLD_REP_UNITS)
RMSE(pls_prediction, dftest$SOLD_REP_UNITS)
RMSE(earth_prediction, dftest$SOLD_REP_UNITS)

View(dftest)
