# Assess Performance of MoveMaker Algorithm


# Check Algorithm performance against actual moves next day (Use TBL_JLP_TOTRACK)

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
      case when cs.prod_id is not null then 1 else nvl(round(ads.likelihood, 4), 0) end as likelihood,
      case when cs.prod_id is not null then 1 else 0 end as confirmed,
      coalesce(cs.demand, round(ads.exp_demand), 0) as exp_demand,
      case when tot.transport_orders is not null then 1 else 0 end as move
    from miniload_stock_statistics mst
      left join TBL_JLP_TOTRACK tot on mst.insert_date + 1 = tot.insert_date and
        mst.licenceplate = tot.licenceplate
      left join average_daily_sales ads on mst.item_number = ads.prod_id and ads.order_type = 'SHOP'
      left join confirmed_sales cs on mst.item_number = cs.prod_id
  "

set.seed(101)

# Because of the biased distribution classes, separate data sets have been generated
# and recombined in hopes of creating a balanced distribution and improving the performance of the model

raw_data <- tbl(simcon, sql(data_sql)) %>%
  collect() %>%
  filter(complete.cases(.) & ITEM_GROUP_NAME != 'KAB')
  


raw_data[c("LOCATION", "Z_POS", "CLASSIFICATION", "DIRECTORATE", "HANGING_GARMENT", "SOR_INDICATOR", "MOVE", "CONFIRMED", "ITEM_GROUP_NAME")] <-
  lapply(raw_data[c("LOCATION", "Z_POS", "CLASSIFICATION", "DIRECTORATE", "HANGING_GARMENT", "SOR_INDICATOR", "MOVE", "CONFIRMED", "ITEM_GROUP_NAME")], as.factor)

# Ensure move = 1 is associated with the positive class, by placing it as the first level
raw_data$MOVE <- relevel(raw_data$MOVE, ref = "1")

dbDisconnect(simcon)


# Load Previously-Trained Model and Make Predictions on Current Data

mod_rf <- readRDS("models/random-forest-model.rds")

predictions <- predict(mod_rf, newdata = raw_data)


# Model Accuracy
confusionMatrix(predictions, raw_data$MOVE)
