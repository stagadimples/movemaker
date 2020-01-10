##################################################################################################
### Author: Isaac Adjei-Attah
### MoveMaker Algorithm
### Objective: 
###  To develop and effective Miniload re-organisation model based on anticipated shop order demand
##################################################################################################

## Begin with all stocked products in the Miniload

## Approach
As this algorithm relies on shop demand for stocked products, so best approach may be to develop a 
product-level forecast.

A simple forecast based on average daily sales * probability of sale is used to determine expected demand

## Assign likelihood of tote selection for totes containing chosen products, and meeting units criteria
+ **Decision Rule**:
+ For each product in ASRS, sort totes in ascending order of units
+ Products in OSR whose units < Expected Units
    + Move required number of totes where total stock >= required difference fro ASRS to OSR

+ Products in OSR whose units >= Expected Units
    + Send extra totes to ASRS, where necessary
    + Do nothing where OSR Units = Expected Units
    
+ To track the accuracy of the algorithm, the table TEST_MINILOAD_REORG has been created in Simulation Server, and 
will capture the output upon execution. This table will be dropped upon completion of the project, as this output will be written directly into IMP_CONT_INFO_MAN in motion.

