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

# Because of the biased distribution classes, separate data sets have been generated
# and recombined in hopes of creating a balanced distribution and improving the performance of the model

raw_data <- tbl(simcon, sql(data_sql)) %>%
  collect() %>%
  select(-INSERT_DATE, -ITEM_NUMBER, -LICENCEPLATE, -LOCNO, -HANGING_GARMENT) %>%
  filter(complete.cases(.))


raw_data$ITEM_GROUP_NAME <- factor(raw_data$ITEM_GROUP_NAME, levels = c("FIF", "FSH", "SHO", "SIO", "SUN", "XXX", "YLG", "KAB"))
raw_data$MOVE <- factor(raw_data$MOVE, levels = c(1, 0))

raw_data[c("LOCATION", "Z_POS", "CLASSIFICATION", "DIRECTORATE", "HANGING_GARMENT", "SOR_INDICATOR", "MOVE", "CONFIRMED")] <-
  lapply(raw_data[c("LOCATION", "Z_POS", "CLASSIFICATION", "DIRECTORATE", "HANGING_GARMENT", "SOR_INDICATOR", "MOVE", "CONFIRMED")], as.factor)


set.seed(102)


intrain <- createDataPartition(raw_data$MOVE, p = 0.80, list = F)

training_set <- raw_data[intrain, ]
test_set <- raw_data[-intrain, ]


# Train Models

# R Part
mod_rp <- train(
  MOVE ~., data = training_set, 
  method = "rpart",
  trControl = trainControl(
    method = "repeatedcv",
    number = 10,
    repeats = 10
  )
)

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


# Support Vector Machines
mod_svm <- train(
  MOVE ~., data = training_set, 
  method = "svmRadial",
  trControl = trainControl(
    method = "repeatedcv",
    number = 10,
    repeats = 10
  )
)


# Generalized Linear Model
mod_glm <- train(
  MOVE ~., data = training_set, 
  method = "glm",
  trControl = trainControl(
    method = "repeatedcv",
    number = 10,
    repeats = 10
  )
)

# Partial Least Squares

mod_c5 <- train(
  MOVE ~., data = training_set, 
  method = "C5.0",
  trControl = trainControl(
    method = "repeatedcv",
    number = 10,
    repeats = 10
  )
)




# Partial Least Squares

mod_pls <- train(
  MOVE ~., data = training_set, 
  method = "pls",
  trControl = trainControl(
    method = "repeatedcv",
    number = 10,
    repeats = 10
  )
)


# Multivariate Adaptive Splines (Earth)

mod_earth <- train(
  MOVE ~., data = training_set, 
  method = "earth",
  trControl = trainControl(
    method = "repeatedcv",
    number = 10,
    repeats = 10
  )
)




# Multivariate Adaptive Splines (Earth)

mod_rp <- train(
  MOVE ~., data = training_set, 
  method = "rpart",
  trControl = trainControl(
    method = "repeatedcv",
    number = 10,
    repeats = 10
  )
)



# Predict Test Sets (Using Trained Models)
prediction_rf <- predict(mod_rf, newdata = test_set)

prediction_glm <- predict(mod_glm, newdata = test_set)

prediction_c5 <- predict(mod_c5, newdata = test_set)

prediction_pls <- predict(mod_pls, newdata = test_set)

prediction_earth <- predict(mod_earth, newdata = test_set)

prediction_svm <- predict(mod_svm, newdata = test_set)

prediction_rp <- predict(mod_rp, newdata = test_set)


# Model Selection (Based on Model Prediction Accuracy)
#############################################################


# Random Forest Model
confusionMatrix(prediction_rf, test_set$MOVE)

# Generalised Linear Model
confusionMatrix(prediction_glm, test_set$MOVE)

# Generalised Linear Model
confusionMatrix(prediction_c5, test_set$MOVE)

# Generalised Linear Model
confusionMatrix(prediction_pls, test_set$MOVE)

# Generalised Linear Model
confusionMatrix(prediction_earth, test_set$MOVE)

# Generalised Linear Model
confusionMatrix(prediction_svm, test_set$MOVE)

# Generalised Linear Model
confusionMatrix(prediction_rp, test_set$MOVE)
# Chosen Model: Random Forest
# In-Sample Accuracy:
# Out-of-Sample Accuracy: 



saveRDS(mod_rf, "models/random-forest-model.rds")

saveRDS(mod_glm, "models/glm-model.rds")

saveRDS(mod_c5, "models/c5-model.rds")

saveRDS(mod_pls, "models/pls-model.rds")

saveRDS(mod_earth, "models/earth-model.rds")

saveRDS(mod_svm, "models/svm-model.rds")

saveRDS(mod_rp, "models/rp-model.rds")

