import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import upper, col
from pyspark.sql.types import StringType

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define source and destination paths
source_path = "s3://group6data1/input/"
destination_path = "s3://group6data1/output/"

# Read from source S3 bucket
source_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [source_path]
    },
    format="csv",
    format_options={
        "withHeader": True,
        "separator": "\t"  # Assuming tab-separated values; change if different
    }
)

# Convert DynamicFrame to DataFrame for transformation
source_df = source_dynamic_frame.toDF()

# Transform the data: Convert all string columns to uppercase
string_cols = [field.name for field in source_df.schema.fields if isinstance(field.dataType, StringType)]
for col_name in string_cols:
    source_df = source_df.withColumn(col_name, upper(col(col_name)))

# Convert back to DynamicFrame
transformed_dynamic_frame = DynamicFrame.fromDF(source_df, glueContext, "transformed_dynamic_frame")

# Write the transformed data to the destination S3 bucket
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": destination_path
    },
    format="csv",
    format_options={
        "separator": "\t",  # Assuming tab-separated values; change if different
        "withHeader": True
    }
)
