import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# Initialize the job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read CSV data from S3 with headers
s3_csv_path = "s3://group6-datalake/output/combine_data.csv"
df = spark.read.option("header", "true").csv(s3_csv_path)

# Schema transformation: Convert columns to appropriate data types
df = df.withColumn("customer_id", F.col("customer_id").cast("int"))
df = df.withColumn("age", F.col("age").cast("int"))
df = df.withColumn("quantity", F.col("quantity").cast("int"))
df = df.withColumn("unit_price", F.col("unit_price").cast("double"))
df = df.withColumn("week_of_year", F.col("week_of_year").cast("int"))
df = df.withColumn("online_purchases", F.col("online_purchases").cast("int"))
df = df.withColumn("in_store_purchases", F.col("in_store_purchases").cast("int"))
df = df.withColumn("total_sales", F.col("total_sales").cast("double"))
df = df.withColumn("product_rating", F.col("product_rating").cast("float"))

# Example transformation: Convert `transaction_date` to a specific date format
df = df.withColumn("transaction_date", F.to_date(F.col("transaction_date"), "dd-MM-yyyy"))

df = df.coalesce(1)

# Write the data back to S3 in Parquet format without overwrite
output_path = "s3://group6-datawarehouse/output/"
df.write.parquet(output_path)

job.commit()
