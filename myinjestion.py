import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Parse job name argument
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Reading the 95% data from S3
s3_data = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://forinjestionjob/part1.csv"], "recurse": True},
    transformation_ctx="s3_data"
)

# Convert DynamicFrame to DataFrame
s3_df = s3_data.toDF()

# Step 2: Reading the 5% data from RDS
jdbc_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://source-database1.ccshp0zc5uef.us-east-1.rds.amazonaws.com:3306/source") \
    .option("dbtable", "customer_transactions") \
    .option("user", "admin") \
    .option("password", "Dnyanesh2326") \
    .load()

# Step 3: Merging the Data
merged_df = s3_df.union(jdbc_df)

# Step 4: Writing the Merged Data to S3 as a Single CSV File
merged_df.coalesce(1).write \
    .format("csv") \
    .option("header", "true") \
    .save("s3a://destinationbucket43/output/combine.csv")

# Commit the job
job.commit()
