select
    nct_id,
    overall_status,
    study_first_submit_year,
    disease,
    has_results,
    study_type,
    phases,
    enrollment_count
from {{ source('clinical_trials', 'silver_trials') }}