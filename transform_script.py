import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue job and context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read CSV from S3
AmazonS3_node1723182686875 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://group6-project-data/input/HI-Small_Trans.csv"], "recurse": True},
    transformation_ctx="AmazonS3_node1723182686875"
)

# Rename Field
RenameField_node1723183052920 = RenameField.apply(
    frame=AmazonS3_node1723182686875, 
    old_name="to bank", 
    new_name="Bank", 
    transformation_ctx="RenameField_node1723183052920"
)

# Convert DynamicFrame to DataFrame
df = RenameField_node1723183052920.toDF()

# Coalesce to a single partition to ensure one output file
df_single_partition = df.coalesce(1)

# Convert back to DynamicFrame
dynamic_frame_single_partition = DynamicFrame.fromDF(df_single_partition, glueContext, "dynamic_frame_single_partition")

# Write the output to S3 as a single CSV file
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_single_partition,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://group6-project-data/output/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1723183749939"
)

# Commit the job
job.commit()
