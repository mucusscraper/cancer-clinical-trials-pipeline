select
    disease,
    overall_status,
    count(*) as total_trials
from {{ ref('stg_trials') }}
group by disease, overall_status