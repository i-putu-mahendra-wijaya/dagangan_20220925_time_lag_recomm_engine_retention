select
    cast(cin.created_at as date) as visit_date
    , cin.user_id
from `x-arcanum-244015.fact.checkin` as cin
where
    cast(cin.created_at as date) >= "2021-01-01"
order by
  cin.partition_created_at asc