#!/bin/bash

set -e

set -a
source ../.env
set +a

export TF_VAR_aws_region="$AWS_REGION"
export TF_VAR_bucket_name="$S3_BUCKET_NAME"

terraform destroy -auto-approve