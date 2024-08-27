import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize the job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Read data from the first S3 source
s3_data1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://s3-raw-source-data/part1.csv"], "recurse": True},
    transformation_ctx="s3_data1"
)

# Step 2: Read data from the Glue Data Catalog (second source)
catalog_data = glueContext.create_dynamic_frame.from_catalog(
    database="database_part2_sql",
    table_name="sql_data_part2",
    transformation_ctx="catalog_data"
)

# Step 3: Convert DynamicFrames to DataFrames for easier manipulation
df_s3_data1 = s3_data1.toDF()
df_catalog_data = catalog_data.toDF()

# Step 4: Combine the data from both DataFrames
combined_df = df_s3_data1.union(df_catalog_data)

# Step 5: Write the combined data back to S3 as a single CSV file (temporary directory)
temp_output_path = "s3://group6-datalake/combined_output_temp/"
combined_df.coalesce(1).write \
    .format("csv") \
    .option("header", "true") \
    .save(temp_output_path)

# Step 6: Rename the part file to combine_data.csv
s3_client = boto3.client('s3')
bucket_name = 'group6-datalake'
temp_folder = 'combined_output_temp/'

# List objects in the temporary output folder
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=temp_folder)
for obj in response['Contents']:
    if obj['Key'].endswith('.csv'):
        temp_file_key = obj['Key']
        # Copy the file to the desired location with the correct name
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': temp_file_key},
            Key='output/combine_data.csv'
        )
        # Delete the temporary file
        s3_client.delete_object(Bucket=bucket_name, Key=temp_file_key)

# Commit the job
job.commit()
