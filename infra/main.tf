resource "aws_s3_bucket" "data_lake" {
  bucket = var.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "data_lake_block" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_glue_catalog_database" "clinical_trials" {
  name = "clinical_trials"
}

resource "aws_glue_catalog_table" "silver_trials" {
  name          = "silver_trials"
  database_name = aws_glue_catalog_database.clinical_trials.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/silver/trials"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "my-stream"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }

    columns {
      name = "nct_id"
      type = "string"
    }

    columns {
      name = "overall_status"
      type = "string"
    }

    columns {
      name = "study_first_submit_date"
      type = "string"
    }

    columns {
      name = "lead_sponsor_name"
      type = "string"
    }
    columns {
      name = "study_type"
      type = "string"
    }
    columns {
      name = "phases"
      type = "array<string>"
    }
    columns {
      name = "enrollment_count"
      type = "int"
    }
    columns {
      name = "sex"
      type = "string"
    }
    columns {
      name = "locations"
      type = "string"
    }
    columns {
      name = "primary_outcomes"
      type = "string"
    }
    columns {
      name = "interventions"
      type = "string"
    }
    columns {
      name = "has_results"
      type = "boolean"
    }
    columns {
      name = "study_first_submit_year"
      type = "int"
    }
  }
  partition_keys {
    name = "disease"
    type = "string"
  }
}