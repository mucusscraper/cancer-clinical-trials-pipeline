select
    disease,
    phases,
    count(*) as total_trials
from {{ ref('stg_trials') }}
group by disease, phases