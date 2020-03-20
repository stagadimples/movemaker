/*
  Revised Performance Measure for MoveMaker
*/
with base
as
(
  select mov.licenceplate
    , mov.insert_date move_date
    , mov.target_state_name
    , mov.act_area_name
    , mov.is_processed
    , nvl(tot.transport_orders, 0)transport_orders
    , tot.insert_date tot_date
  from tbl_jlp_movetrack mov
    left join tbl_jlp_totrack tot on mov.licenceplate = tot.licenceplate and mov.insert_date + 1 = tot.insert_date
  where mov.insert_date between trunc(sysdate) - 7 and trunc(sysdate) - 1
)
select move_date
  , totes
  , success
from
(
  select move_date
    , classification
    , count(distinct licenceplate)totes
    , round(count(distinct licenceplate) / (sum(count(distinct licenceplate)) over (partition by move_date)), 4)success
  from
  (
    select base.*
      , case
          when target_state_name = 'Z-MOVE_OSR' and 
            is_processed = 'Y' and
            transport_orders < 1
          then 'Failure'   
          when target_state_name = 'Z-MOVE_OSR' and 
            is_processed = 'Y' and
            transport_orders > 0
          then 'Success' 
          when target_state_name = 'Z-MOVE_OSR' and 
            is_processed = 'N' and
            transport_orders > 0
          then 'Success'
          when target_state_name = 'Z-MOVE_OSR' and 
            is_processed = 'N' and
            transport_orders = 0
          then 'Failure' 
          when target_state_name = 'Z-MOVE_OSR' and 
            is_processed = 'W'
          then 'Success'
          when target_state_name = 'Z-MOVE_OSR' and 
            is_processed = 'R'
          then 'Failure' 
          when target_state_name = 'Z-MOVE_ASRS' and 
            is_processed = 'Y'
            and transport_orders > 0
          then 'Failure' 
          when target_state_name = 'Z-MOVE_ASRS' and 
            is_processed = 'Y'
            and transport_orders = 0
          then 'Success'
          when target_state_name = 'Z-MOVE_ASRS' and 
            is_processed = 'N'
            and transport_orders = 0
          then 'Success'
          when target_state_name = 'Z-MOVE_ASRS' and 
            is_processed = 'N'
            and transport_orders > 0
          then 'Failure' 
          when target_state_name = 'Z-MOVE_ASRS' and 
            is_processed = 'W'
            and transport_orders > 0
          then 'Failure' 
          when target_state_name = 'Z-MOVE_ASRS' and 
            is_processed = 'R'
          then 'Success'  
        end classification
    from base
  )
  group by move_date
    , classification
)
where classification = 'Success'