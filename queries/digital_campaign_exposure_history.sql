select
  cast(adj.timestamp as date) as observation_date
  , adj.userId as user_id
  , adj.network
from `x-arcanum-244015.misc.adjust` as adj
where
  adj.network not in ("Unattributed", "Organic")
order by
  adj.timestamp desc