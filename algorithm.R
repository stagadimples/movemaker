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

data_sql <- 
  "
    select 
      mst.*,
      case when tot.transport_orders is not null then 1 else 0 end as move
    from miniload_stock_statistics mst
      left join TBL_JLP_TOTRACK tot on mst.insert_date = tot.insert_date and
        mst.licenceplate = tot.licenceplate
  "


raw_data <- tbl(simcon, sql(data_sql)) %>%
  collect() %>%
  select(-INSERT_DATE, -ITEM_NUMBER, -LICENCEPLATE, -LOCNO) %>%
  filter(complete.cases(.))


raw_data[c("LOCATION", "Z_POS", "CLASSIFICATION", "DIRECTORATE", "HANGING_GARMENT", "SOR_INDICATOR", "MOVE")] <-
  lapply(raw_data[c("LOCATION", "Z_POS", "CLASSIFICATION", "DIRECTORATE", "HANGING_GARMENT", "SOR_INDICATOR", "MOVE")], as.factor)

intrain <- createDataPartition(raw_data$MOVE, p = 0.75, list = F)

training_set <- raw_data[intrain, ]
test_set <- raw_data[-intrain, ]


mod_rp <- train(MOVE ~., data = training_set, method="rpart")

prediction_rp <- predict(mod_rp, newdata = test_set)
