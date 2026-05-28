#!/bin/bash
set -e

mkdir -p /root/.dbt

cat > /root/.dbt/profiles.yml <<EOF
clinical_trials_dbt:
  outputs:
    dev:
      type: athena
      s3_staging_dir: s3://${S3_BUCKET_NAME}/athena-results/
      s3_data_dir: s3://${S3_BUCKET_NAME}/dbt/
      region_name: ${AWS_REGION}
      schema: clinical_trials
      database: awsdatacatalog
      threads: 4

  target: dev
EOF

echo "Running dbt..."

dbt run

echo "Running tests..."

dbt test