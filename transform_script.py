
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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
df = df.withColumn("Order ID", F.col("Order ID").cast("string"))

df = df.withColumn("Ship Mode", F.col("Ship Mode").cast("string"))

df = df.withColumn("Customer ID", F.col("Customer ID").cast("string"))

df = df.withColumn("Customer Name", F.col("Customer Name").cast("string"))

df = df.withColumn("Segment", F.col("Segment").cast("string"))

df = df.withColumn("City", F.col("City").cast("string"))

df = df.withColumn("State", F.col("State").cast("string"))

df = df.withColumn("Country", F.col("Country").cast("string"))

df = df.withColumn("Market", F.col("Market").cast("string"))

df = df.withColumn("Region", F.col("Region").cast("string"))

df = df.withColumn("Product ID", F.col("Product ID").cast("string"))

df = df.withColumn("Category", F.col("Category").cast("string"))

df = df.withColumn("Product Name", F.col("Product Name").cast("string"))

df = df.withColumn("Sales", F.round(F.col("Sales").cast("float"), 2))

df = df.withColumn("Quantity", F.col("Quantity").cast("int"))

df = df.withColumn("Profit", F.col("Profit").cast("double"))

df = df.withColumn("Shipping Cost", F.col("Shipping Cost").cast("float"))

df = df.withColumn("Order Priority", F.col("Order Priority").cast("string"))

df = df.withColumnRenamed("Order ID", "Order_ID")

df = df.withColumnRenamed("Ship Mode", "Ship_Mode")

df = df.withColumnRenamed("Customer ID", "Customer_ID")

df = df.withColumnRenamed("Customer Name", "Customer_Name")

df = df.withColumnRenamed("Product ID", "Product_ID")

df = df.withColumnRenamed("Order Priority", "Order_Priority")

df = df.withColumnRenamed("order_date", "Order_Date")

# Example transformation: Convert `Order Date` to a specific date format
df = df.withColumn("Order_Date", F.to_date(F.col("Order_Date"), "dd-MM-yyyy"))

# Assuming you have a column named 'date' in your DataFrame
df = df.withColumn("year", F.year(F.col("Order_Date")))

df = df.withColumn("month_name", F.date_format(F.col("Order_Date"), "MMMM"))

df = df.withColumn("week_day", F.date_format(F.col("Order_Date"), "EEEE"))

df = df.withColumn("Cost_Price", F.col("Shipping Cost") + F.col("Sales"))

cols_to_drop = ["Row ID", "Shipping Cost", "Ship Date","Postal Code"]

df = df.drop(*cols_to_drop)

df.printSchema()

df = df.coalesce(1)

# Write the data back to S3 in Parquet format without overwrite
output_path = "s3://group6-datawarehouse/output/"
df.write.parquet(output_path, mode="overwrite")

job.commit()


