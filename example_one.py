import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

print("Starting Glue Job: Script 1 - RAW CSV to PROCESSED Parquet")

# Read job parameters passed from Glue Console
args = getResolvedOptions(sys.argv, ["JOB_NAME", "SRC_PATH", "PROCESSED_PATH"])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue Job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Input path: RAW layer CSV location in S3
src = args["SRC_PATH"]

# Output path: PROCESSED layer Parquet location in S3
processed = args["PROCESSED_PATH"]

print("Input RAW Path:", src)
print("Output PROCESSED Path:", processed)

# Step 1: Read CSV data from S3 RAW layer
print("Step 1: Reading CSV file from RAW layer")

df = (
    spark.read
    .option("header", "true")          # First row contains column names
    .option("inferSchema", "true")     # Automatically detect column data types
    .csv(src)                         # Load CSV file from S3 path
)

print("CSV Read Completed Successfully")
print("Total Records in RAW file:", df.count())

# Step 2: Apply transformations and standardize the dataset
print("Step 2: Applying transformations")

df_t = (
    df

    # Convert user_id from string to integer for consistency
    .withColumn("user_id", F.col("user_id").cast("int"))

    # Remove leading/trailing spaces from username
    .withColumn("username", F.trim(F.col("username")))

    # Convert email to lowercase and remove extra spaces
    .withColumn("email", F.lower(F.trim(F.col("email"))))

    # Standardize city names (example: kansas city → Kansas City)
    .withColumn("city", F.initcap(F.trim(F.col("city"))))

    # Convert signup_date column into proper date format
    .withColumn("signup_date", F.to_date(F.col("signup_date"), "M/d/yy"))


    # Convert is_active column into boolean type
    .withColumn("is_active", F.col("is_active").cast("boolean"))

    # Add ingestion timestamp to track when the record was processed
    .withColumn("ingest_ts", F.current_timestamp())
)

print("Transformations Completed Successfully")

# Step 3: Remove duplicate records based on user_id
print("Step 3: Removing duplicates based on user_id")

df_t = df_t.dropDuplicates(["user_id"])

print("Total Records After Deduplication:", df_t.count())

# Step 4: Write cleaned output as Parquet into PROCESSED layer
print("Step 4: Writing output to PROCESSED layer in Parquet format")

df_t.write \
    .mode("append") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(processed)

print("Parquet Write Completed Successfully")

# Commit the Glue job
job.commit()

print("Glue Job Completed Successfully")