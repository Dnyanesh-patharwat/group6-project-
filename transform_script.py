import sys
from awsglue.transforms import RenameField  # Import specific functions you need
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1723182686875 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": "\"",
        "withHeader": True,
        "separator": ",", 
        "optimizePerformance": False
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://group6-project-data/input/HI-Small_Trans.csv"], 
        "recurse": True
    },
    transformation_ctx="AmazonS3_node1723182686875"
)

# Script generated for node Rename Field
RenameField_node1723183052920 = RenameField.apply(
    frame=AmazonS3_node1723182686875,
    old_name="to bank",
    new_name="Bank",
    transformation_ctx="RenameField_node1723183052920"
)

# Script generated for node Drop Duplicates
DropDuplicates_node1723183698719 = DynamicFrame.fromDF(
    RenameField_node1723183052920.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1723183698719"
)

# Coalesce to a single partition to write a single output file
coalesced_df = DropDuplicates_node1723183698719.toDF().coalesce(1)

# Convert back to DynamicFrame
col_dyn_fr = DynamicFrame.fromDF(coalesced_df, glueContext, "coldyn_fr")

# Script generated for node Amazon S3
AmazonS3_node1723183749939 = glueContext.write_dynamic_frame.from_options(
    frame=col_dyn_fr,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://group6-output-data", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1723183749939"
)

job.commit()
