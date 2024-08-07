import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

# Initialize a Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the S3 paths
input_path = 's3://groupdata6/input/data.csv'
output_path = 's3://outputbucket6'

# Create a dynamic frame from the CSV files
input_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [input_path]},
    transformation_ctx="input_dynamic_frame"
)

# Transform the dynamic frame as needed (this example does not apply any transformations)
# If you need transformations, add them here

# Write the dynamic frame to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=input_dynamic_frame,
    connection_type="s3",
    format="parquet",
    connection_options={"path": output_path},
    transformation_ctx="output_dynamic_frame"
)

# Commit the job
job.commit()
