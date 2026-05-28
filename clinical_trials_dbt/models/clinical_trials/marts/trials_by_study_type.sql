select
    disease,
    study_type,
    count(*) as total_trials
from {{ ref('stg_trials') }}
group by disease, study_type