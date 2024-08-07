import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

## Initialize Glue context
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

## Define input and output locations
input_path = "s3://group6data1/input/data1.csv"  # Replace with your input path
output_path = "s3://group6data1/output/"  # Replace with your output path

## Read the CSV file
data_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [input_path]},
    transformation_ctx="data_frame",
)

## Function to convert string fields to uppercase
def to_upper_case(rec):
    for key in rec.keys():
        if isinstance(rec[key], str):  # Check if the value is a string
            rec[key] = rec[key].upper()
    return rec

## Map transformation to apply to_upper_case to each record
mapped_frame = Map.apply(frame=data_frame, f=to_upper_case)

## Write the transformed data back to S3
glueContext.write_dynamic_frame.from_options(
    frame=mapped_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="csv",
    transformation_ctx="mapped_frame",
)

