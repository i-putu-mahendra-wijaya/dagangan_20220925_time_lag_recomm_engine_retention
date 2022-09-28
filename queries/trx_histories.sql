with
  trx_item_summary as (
    select
      tri.trx_id
      , count(distinct tri.sku_id) as count_sku
      , sum(tri.quantity) as sum_quantity
      , count(distinct tri.category_name) as count_category
      , count(distinct tri.brand_name) as count_brand
      , count(distinct tri.promo_id) as count_promo_usage
      , sum(tri.discount) as sum_discount
    from `x-arcanum-244015.fact.trx_item` as tri
    where
      tri.deleted is null
    group by
      tri.trx_id
  )

select
  cast(trx.paid_at as date) as trx_date
  , trx.id as trx_id
  , trx.is_first_trx
  , trx.user_id
  , usr.sales_id
  , sls.name as sales_name
  , cast(usr.created_at as date) as registration_date
  , trx.warehouse_id
  , trx.hub_name as warehouse_name
  , trx.kelurahan_name
  , trx.kecamatan_name
  , trx.kabupaten_name
  , trx.coupon_code
  , trx.final_price
  , trx.coupon_price
  , trx.point_usage
  , trx.device_id
  , tri.count_sku
  , tri.sum_quantity
  , tri.count_category
  , tri.count_brand
  , tri.count_promo_usage
  , tri.sum_discount
  , coalesce(trx.coupon_price,0) + coalesce(tri.sum_discount,0) + coalesce(trx.point_usage,0) as sum_reduction_component
from `x-arcanum-244015.fact.trx` as trx
inner join trx_item_summary as tri on trx.id = tri.trx_id
inner join `x-arcanum-244015.dim.user` as usr on trx.user_id = usr.id
left join `x-arcanum-244015.dim.sales` as sls on usr.sales_id = sls.id
where
  trx.user_id not in (
    select
      excl.user_id
    from `x-arcanum-244015.misc.user_exclusion_vw` as excl
  )
  and trx.utm_medium not in ("external-api")
  and trx.status in ("processed","in_delivery","delivered")
  and cast(trx.paid_at as date) is not null
  and cast(trx.paid_at as date) >= "2021-01-01"
  and cast(usr.created_at as date) >= "2021-01-01"
order by
  user_id asc
  , trx_date asc

