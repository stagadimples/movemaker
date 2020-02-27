drop table shop_demand_training_data;
create table shop_demand_training_data
as
with order_groups as (
  select 
    prod_id,
    sum(case when orderdate between trunc(sysdate) - 8 and trunc(sysdate) - 2 then orders else 0 end) as rep_orders_lst7,
    sum(case when orderdate between trunc(sysdate) - 8 and trunc(sysdate) - 2 then units else 0 end) as rep_units_lst7,
    round(sum(case when orderdate between trunc(sysdate) - 8 and trunc(sysdate) - 2 then orders else 0 end) / 7, 4) as avg_rep_orders_lst7,
    round(sum(case when orderdate between trunc(sysdate) - 8 and trunc(sysdate) - 2 then units else 0 end) / 7, 4) as avg_rep_units_lst7,
    round(sum(case when orderdate between trunc(sysdate) - 15 and trunc(sysdate) - 2 then orders else 0 end) / 14, 4) as avg_rep_orders_lst14,
    round(sum(case when orderdate between trunc(sysdate) - 15 and trunc(sysdate) - 2 then units else 0 end) / 14, 4) as avg_rep_units_lst14,
    round(sum(case when orderdate between trunc(sysdate) - 31 and trunc(sysdate) - 2 then orders else 0 end) / 30, 4) as avg_rep_orders_lst30,
    round(sum(case when orderdate between trunc(sysdate) - 31 and trunc(sysdate) - 2 then units else 0 end) / 30, 4) as avg_rep_units_lst30
  from
  (
    select 
      trunc(orh.creation_time) as orderdate,
      orh.order_type,
      item.system_product_id as prod_id,
      count(distinct orh.id) as orders,
      sum(orl.requested_quantity) as units
    from (
        select * from originator_headers@lwms
        union all
        select * from k1233kw_dw.originator_headers@lwms
      ) orh
      inner join (
        select * from originator_lines@lwms
        union
        select * from k1233kw_dw.originator_lines@lwms
      ) orl on orh.id = orl.originator_header_id
      inner join products@lwms item on orl.item_id = item.id
    where
      orh.order_type = 0 and
      trunc(orh.creation_time) between trunc(sysdate) - 31 and trunc(sysdate) - 2
    group by
      trunc(orh.creation_time),
      orh.order_type,
      item.system_product_id
  )
  group by
    prod_id
), stock_groups as (
  select 
    prod_id,
    round(avg(stock), 4) as avg_stock_lst7,
    round(avg(case when snap_date between trunc(sysdate) - 15 and trunc(sysdate) - 2 then stock end), 4) as avg_stock_lst14,
    round(avg(case when snap_date between trunc(sysdate) - 31 and trunc(sysdate) - 2 then stock end), 4) as avg_stock_lst30,
    max(case when snap_date = trunc(sysdate) - 2 then stock else 0 end)current_stock
  from
  (
    select 
      snap_date,
      prod_id,
      sum(units)stock
    from archive_stockonmo
    where snap_date between trunc(sysdate) - 31 and trunc(sysdate) - 2
    group by
      snap_date,
      prod_id
  )
  group by prod_id
)
select *
from
(
  select
    trunc(sysdate) - 1 as insert_date,
    p.prod_id,
    dis.directorate,
    p.hanging_garment,
    p.brom_size,
    p.sor_indicator,
    dis.partner_discount,
    p.uplb,
    round(p.length * p.width * p.height / 1000000, 4)volume,
    p.selling_price / 100 selling_price,
    s.current_stock,
    nvl(s.avg_stock_lst7, 0) as avg_stock_lst7,
    nvl(s.avg_stock_lst14, 0) as avg_stock_lst14,
    nvl(s.avg_stock_lst30, 0) as avg_stock_lst30,
    nvl(o.rep_orders_lst7, 0) as rep_orders_lst7,
    nvl(o.rep_units_lst7, 0) as rep_units_lst7,
    nvl(o.avg_rep_orders_lst7, 0) as avg_rep_orders_lst7,
    nvl(o.avg_rep_units_lst7, 0) as avg_rep_units_lst7,
    nvl(o.avg_rep_orders_lst14, 0) as avg_rep_orders_lst14,
    nvl(o.avg_rep_units_lst14, 0) as avg_rep_units_lst14,
    nvl(o.avg_rep_orders_lst30, 0) as avg_rep_orders_lst30,
    nvl(o.avg_rep_units_lst30, 0) as avg_rep_units_lst30
  from 
    current_kisoft_products p
    inner join stock_groups s on p.prod_id = s.prod_id
    inner join tbl_dissections_new dis on substr(p.prod_code, 1, 3) = dis.dissection
    left join order_groups o on p.prod_id = o.prod_id
  where coalesce(o.prod_id, s.prod_id) is not null
    and p.state = 1
    and s.current_stock > 0
)