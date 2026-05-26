#!/usr/bin/env python
# coding: utf-8

# In[2]:


import boto3
import os
from pyspark.sql.functions import col, to_date, substring, current_timestamp, explode, lit
from pyspark.sql import SparkSession


# In[3]:


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


# In[4]:


bucket_name = os.environ.get("S3_BUCKET_NAME")


# In[5]:


s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)


# In[6]:


response = s3.list_objects_v2(
    Bucket=bucket_name,
    Prefix="raw/",
    Delimiter="/"
)


# In[7]:


diseases = [
    prefix["Prefix"].split("/")[1]
    for prefix in response["CommonPrefixes"]
]


# In[8]:


for disease in diseases:
    disease_path = (
        f"s3a://{bucket_name}/raw/{disease}/*.json"
    )
    df = (
        spark.read
        .option("multiline", "true")
        .json(disease_path)
    )
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

    col("study.protocolSection.contactsLocationsModule.locations")
        .alias("locations"),

    col("study.protocolSection.outcomesModule.primaryOutcomes")
        .alias("primary_outcomes"),

    col("study.protocolSection.armsInterventionsModule.interventions")
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
        .mode("overwrite")
        .partitionBy("disease")
        .parquet(f"s3a://{bucket_name}/silver/trials/{disease}/")
    )



