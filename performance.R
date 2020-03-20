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
        mst.licenceplate = tot.licenceplate and
        tot.insert_date = trunc(sysdate)
      left join average_daily_sales ads on mst.item_number = ads.prod_id and ads.order_type = 'SHOP'
      left join confirmed_sales cs on mst.item_number = cs.prod_id
  "

# Because of the biased distribution classes, separate data sets have been generated
# and recombined in hopes of creating a balanced distribution and improving the performance of the model

raw_data <- tbl(simcon, sql(data_sql)) %>%
  collect()%>%
  select(-INSERT_DATE, -ITEM_NUMBER, -LICENCEPLATE, -LOCNO, -HANGING_GARMENT) %>%
  filter(complete.cases(.))
  

raw_data$ITEM_GROUP_NAME <- factor(raw_data$ITEM_GROUP_NAME, levels = c("FIF", "FSH", "SHO", "SIO", "SUN", "XXX", "YLG", "KAB"))
raw_data$MOVE <- factor(raw_data$MOVE, levels = c(1, 0))

raw_data[c("LOCATION", "Z_POS", "CLASSIFICATION", "DIRECTORATE", "HANGING_GARMENT", "SOR_INDICATOR", "MOVE", "CONFIRMED")] <-
  lapply(raw_data[c("LOCATION", "Z_POS", "CLASSIFICATION", "DIRECTORATE", "HANGING_GARMENT", "SOR_INDICATOR", "MOVE", "CONFIRMED")], as.factor)

dbDisconnect(simcon)


# Load Previously-Trained Model and Make Predictions on Current Data

mod_rf <- readRDS("models/random-forest-model.rds")
predictions_rf <- predict(mod_rf, newdata = raw_data)

mod_glm <- readRDS("models/glm-model.rds")
predictions_glm <- predict(mod_glm, newdata = raw_data)

mod_c5 <- readRDS("models/glm-model.rds")
predictions_c5 <- predict(mod_c5, newdata = raw_data)

mod_pls <- readRDS("models/pls-model.rds")
predictions_pls <- predict(mod_pls, newdata = raw_data)

mod_earth <- readRDS("models/earth-model.rds")
predictions_earth <- predict(mod_earth, newdata = raw_data)

mod_svm <- readRDS("models/svm-model.rds")
predictions_svm <- predict(mod_svm, newdata = raw_data)

mod_rp <- readRDS("models/rp-model.rds")
predictions_rp <- predict(mod_rp, newdata = raw_data)


# Model Accuracy
confusionMatrix(predictions_rf, raw_data$MOVE)

confusionMatrix(predictions_glm, raw_data$MOVE)

confusionMatrix(predictions_c5, raw_data$MOVE)

confusionMatrix(predictions_pls, raw_data$MOVE)

confusionMatrix(predictions_earth, raw_data$MOVE)

confusionMatrix(predictions_svm, raw_data$MOVE)

confusionMatrix(predictions_rp, raw_data$MOVE)
