with classified_trials as (

    select
        disease,

        case
            when enrollment_count is null then 'Not Specified'
            when enrollment_count = 0 then '0'
            when enrollment_count between 1 and 10 then '1-10'
            when enrollment_count between 11 and 50 then '11-50'
            when enrollment_count between 51 and 100 then '51-100'
            when enrollment_count between 101 and 500 then '101-500'
            when enrollment_count between 501 and 1000 then '501-1000'
            when enrollment_count > 1000 then '>1000'
        end as enrollment_class

    from {{ ref('stg_trials') }}

)

select
    disease,
    enrollment_class,
    count(*) as total_trials

from classified_trials

group by
    disease,
    enrollment_class

order by
    disease,
    total_trials desc