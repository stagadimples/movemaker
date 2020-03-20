/*
  CREATE OR REPLACE FORCE VIEW V_JLP_MOVEMAKERPERF
  AS
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
  , substr(target_state_name, 8, 4)target_area
  , classification
  , count(distinct licenceplate)totes
  , sum(count(distinct licenceplate)) over (partition by move_date)totals
  , round(count(distinct licenceplate) / (sum(count(distinct licenceplate)) over (partition by move_date)), 4)p_area_totes
from
(
  select base.*
    , case
        when target_state_name = 'Z-MOVE_OSR' and 
          is_processed = 'Y' and
          transport_orders < 1
        then '3. B'  
        when target_state_name = 'Z-MOVE_OSR' and 
          is_processed = 'Y' and
          transport_orders > 0
        then '1. H' 
        when target_state_name = 'Z-MOVE_OSR' and 
          is_processed = 'N' and
          transport_orders > 0
        then '2. H U'
        when target_state_name = 'Z-MOVE_OSR' and 
          is_processed = 'N' and
          transport_orders = 0
        then '4. B L'
        when target_state_name = 'Z-MOVE_OSR' and 
          is_processed = 'W'
        then '1. H'
        when target_state_name = 'Z-MOVE_OSR' and 
          is_processed = 'R'
        then '3. B'
        when target_state_name = 'Z-MOVE_ASRS' and 
          is_processed = 'Y'
          and transport_orders > 0
        then '7. B'
        when target_state_name = 'Z-MOVE_ASRS' and 
          is_processed = 'Y'
          and transport_orders = 0
        then '5. H'
        when target_state_name = 'Z-MOVE_ASRS' and 
          is_processed = 'N'
          and transport_orders = 0
        then '6. H U'
        when target_state_name = 'Z-MOVE_ASRS' and 
          is_processed = 'N'
          and transport_orders > 0
        then '8. B L' 
        when target_state_name = 'Z-MOVE_ASRS' and 
          is_processed = 'W'
          and transport_orders > 0
        then '8. B L' 
        when target_state_name = 'Z-MOVE_ASRS' and 
          is_processed = 'R'
        then '5. H'  
      end classification
  from base
)
group by move_date
  , substr(target_state_name, 8, 4)
  , classification