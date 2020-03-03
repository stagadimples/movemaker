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

raw_data1 <- tbl(simcon, sql(data_sql)) %>%
  collect() %>%
  filter(MOVE == 0) %>%
  slice(1:5000) %>%
  select(-INSERT_DATE, -ITEM_NUMBER, -LICENCEPLATE, -LOCNO) %>%
  filter(complete.cases(.))


raw_data2 <- tbl(simcon, sql(data_sql)) %>%
  collect() %>%
  filter(MOVE == 1) %>%
  slice(1:5000) %>%
  select(-INSERT_DATE, -ITEM_NUMBER, -LICENCEPLATE, -LOCNO) %>%
  filter(complete.cases(.))


raw_data <- rbind(raw_data1, raw_data2)


raw_data[c("LOCATION", "Z_POS", "CLASSIFICATION", "DIRECTORATE", "HANGING_GARMENT", "SOR_INDICATOR", "MOVE", "CONFIRMED", "ITEM_GROUP_NAME")] <-
  lapply(raw_data[c("LOCATION", "Z_POS", "CLASSIFICATION", "DIRECTORATE", "HANGING_GARMENT", "SOR_INDICATOR", "MOVE", "CONFIRMED", "ITEM_GROUP_NAME")], as.factor)




set.seed(102)


intrain <- createDataPartition(raw_data$MOVE, p = 0.80, list = F)

training_set <- raw_data[intrain, ]
test_set <- raw_data[-intrain, ]


# Train Models


# Random Forest
mod_rf <- train(
  MOVE ~., data = training_set, 
  method = "rf",
  trControl = trainControl(
                  method = "repeatedcv",
                  number = 10,
                  repeats = 10
                  )
  )


# Predict Test Sets (Using Trained Models)
prediction_rf <- predict(mod_rf, newdata = test_set)


# Model Selection (Based on Model Prediction Accuracy)
#############################################################


# Random Forest Model
confusionMatrix(prediction_rf, test_set$MOVE)


# Chosen Model: Random Forest
# In-Sample Accuracy:
# Out-of-Sample Accuracy: 



saveRDS(mod_rf, "models/random-forest-model.rds")

