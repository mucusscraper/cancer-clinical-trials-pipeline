select
    disease,
    count(distinct nct_id) as total_trials
from {{ ref('stg_trials') }}
group by disease