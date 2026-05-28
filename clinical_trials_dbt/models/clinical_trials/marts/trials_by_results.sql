select
    disease,
    has_results,
    count(*) as total_trials
from {{ ref('stg_trials') }}
group by disease, has_results