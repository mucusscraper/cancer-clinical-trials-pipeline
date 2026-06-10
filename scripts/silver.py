#!/usr/bin/env python
# coding: utf-8

import boto3
import os
from pyspark.sql.functions import col, to_date, substring, current_timestamp, explode, lit, to_json
from pyspark.sql import SparkSession


# Create a SparkSession
spark = (
    SparkSession.builder
    .appName("cancer-clinical-trials")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .getOrCreate()
)


bucket_name = os.environ.get("S3_BUCKET_NAME")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)


response = s3.list_objects_v2(
    Bucket=bucket_name,
    Prefix="raw/",
    Delimiter="/"
)

paginator = s3.get_paginator("list_objects_v2")

objects_to_delete = []

for page in paginator.paginate(
    Bucket=bucket_name,
    Prefix="silver/trials/"
):
    if "Contents" not in page:
        continue

    for obj in page["Contents"]:
        objects_to_delete.append(
            {"Key": obj["Key"]}
        )

for i in range(0, len(objects_to_delete), 1000):
    batch = objects_to_delete[i:i + 1000]

    s3.delete_objects(
        Bucket=bucket_name,
        Delete={
            "Objects": batch
        }
    )

print(
    f"Deleted {len(objects_to_delete)} objects from silver/trials/"
)

diseases = [
    prefix["Prefix"].split("/")[1]
    for prefix in response["CommonPrefixes"]
]
print(response)
print("Diseases found:", diseases)

for disease in diseases:
    disease_path = (
        f"s3a://{bucket_name}/raw/{disease}/*.json"
    )
    df = (
        spark.read
        .option("multiline", "true")
        .json(disease_path)
    )
    print(f"Processing disease: {disease}")
    df.printSchema()
    df.show(1, truncate=False)
    df = df.withColumn(
        "study",
        explode("studies")
    )
    flat_df = df.select(
    col("study.protocolSection.identificationModule.nctId").alias("nct_id"),

    col("study.protocolSection.statusModule.overallStatus")
        .alias("overall_status"),

    col("study.protocolSection.statusModule.studyFirstSubmitDate")
        .alias("study_first_submit_date"),

    col("study.protocolSection.sponsorCollaboratorsModule.leadSponsor.name")
        .alias("lead_sponsor_name"),

    col("study.protocolSection.designModule.studyType")
        .alias("study_type"),

    col("study.protocolSection.designModule.phases")
        .alias("phases"),

    col("study.protocolSection.designModule.enrollmentInfo.count")
        .alias("enrollment_count"),

    col("study.protocolSection.eligibilityModule.sex")
        .alias("sex"),

    to_json(col("study.protocolSection.contactsLocationsModule.locations"))
    .alias("locations"),

    to_json(col("study.protocolSection.outcomesModule.primaryOutcomes"))
    .alias("primary_outcomes"),

    to_json(col("study.protocolSection.armsInterventionsModule.interventions"))
    .alias("interventions"),

    col("study.hasResults")
        .alias("has_results")
    )
    flat_df = (
    flat_df
        .filter(col("nct_id").isNotNull())
        .dropDuplicates(["nct_id"])
        .withColumn("study_first_submit_year",
            substring(col("study_first_submit_date"),
            1,
            4
            ).cast("int")
        )
    )
    flat_df = flat_df.withColumn(
        "disease",
        lit(disease)
    )
    (
    flat_df
        .write
        .mode("append")
        .partitionBy("disease")
        .parquet(f"s3a://{bucket_name}/silver/trials/")
    )

athena = boto3.client("athena", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"), region_name=os.getenv("AWS_REGION"))

athena.start_query_execution(
    QueryString=f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS clinical_trials.silver_trials (
        nct_id string,
        overall_status string,
        study_first_submit_date string,
        lead_sponsor_name string,
        study_type string,
        phases string,
        enrollment_count bigint,
        sex string,
        locations string,
        primary_outcomes string,
        interventions string,
        has_results boolean,
        study_first_submit_year int
    )
    PARTITIONED BY (
        disease string
    )
    STORED AS PARQUET
    LOCATION 's3://{bucket_name}/silver/trials/'
    """,
    ResultConfiguration={
        "OutputLocation": f"s3://{bucket_name}/athena-results/"
    }
)

athena.start_query_execution(
    QueryString="""
    MSCK REPAIR TABLE clinical_trials.silver_trials
    """,
    ResultConfiguration={
        "OutputLocation": f"s3://{bucket_name}/athena-results/"
    }
)


