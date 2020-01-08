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

## Decision Rule:
# A. For each product in ASRS, sort totes in descending order of units


# 1. Products in OSR whose units < Expected Units
#   a. Move required number of totes where total stock >= required difference fro ASRS to OSR


# 2. Products in OSR whose units >= Expected Units
#   a. Send extra totes to ASRS, where necessary
#   b. Do nothing where OSR Units = Expected Units

