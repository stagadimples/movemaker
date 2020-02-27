/* Performance of MoveMaker Algorithm */

select
  mov.*,
  tot.transport_orders
from test_miniload_reorg mov
  left join V_JLP_TOTRACK@lwms tot on 
    mov.licenceplate = tot.licenceplate and
     trunc(mov.timestamp) + 1 = tot.insert_date
where trunc(mov.timestamp) = trunc(sysdate) - 1