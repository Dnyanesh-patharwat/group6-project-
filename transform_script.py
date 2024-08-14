import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1723638198474 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://group6-datalake/output/combine_data.csv"], "recurse": True},
    transformation_ctx="AmazonS3_node1723638198474"
)

# Script generated for node Change Schema
ChangeSchema_node1723638356856 = ApplyMapping.apply(
    frame=AmazonS3_node1723638198474,
    mappings=[
        ("customer_id", "string", "customer_id", "int"),
        ("age", "string", "age", "int"),
        ("gender", "string", "gender", "string"),
        ("transaction_date", "string", "transaction_date", "string"),  # Keep as string initially
        ("quantity", "string", "quantity", "int"),
        ("unit_price", "string", "unit_price", "double"),  # Changed to double
        ("store_location", "string", "store_location", "string"),
        ("week_of_year", "string", "week_of_year", "int"),  # Changed to int
        ("purchase_frequency", "string", "purchase_frequency", "string"),
        ("online_purchases", "string", "online_purchases", "string"),
        ("in_store_purchases", "string", "in_store_purchases", "timestamp"),
        ("total_sales", "string", "total_sales", "double"),  # Changed to double
        ("product_rating", "string", "product_rating", "float"),
        ("season", "string", "season", "string")
    ],
    transformation_ctx="ChangeSchema_node1723638356856"
)

# Convert DynamicFrame to DataFrame
df = ChangeSchema_node1723638356856.toDF()

# Example transformation: Convert `transaction_date` to a specific date format
df = df.withColumn("transaction_date", F.to_date(F.col("transaction_date"), "dd-MM-yyyy"))

# Example transformation: Add a new column `total_amount` calculated from `quantity` and `unit_price`
df = df.withColumn("total_amount", F.col("quantity") * F.col("unit_price"))

# Coalesce the DataFrame into a single partition (one file)
df = df.coalesce(1)

# Convert DataFrame back to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Write the coalesced data back to S3
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://group6-datawarehouse/output/", "partitionKeys": []},
    format="parquet",
    format_options={"compression": "UNCOMPRESSED"}  # Specify uncompressed format
)

job.commit()
