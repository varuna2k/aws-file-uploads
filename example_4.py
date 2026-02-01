import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

print("Starting Glue Job: Script 4 - CURATED Parquet -> Redshift (Create Table + Load)")
print("Note: Redshift connection is attached in Glue Job UI (no connectionName param used)")

# Read job parameters passed from Glue Console
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "CURATED_PATH",
    "REDSHIFT_DB",
    "REDSHIFT_SCHEMA",
    "REDSHIFT_TABLE",
    "REDSHIFT_TMP_DIR"
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue Job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

curated_path = args["CURATED_PATH"]          # S3 CURATED layer path (partitioned)
redshift_db = args["REDSHIFT_DB"]            # Redshift database name (example: dev)
redshift_schema = args["REDSHIFT_SCHEMA"]    # Redshift schema (example: public)
redshift_table = args["REDSHIFT_TABLE"]      # Redshift table name (example: curated_users)
redshift_tmp_dir = args["REDSHIFT_TMP_DIR"]  # S3 temp dir required by Glue Redshift writer

users_table = f"{redshift_schema}.{redshift_table}"

print("Input CURATED Path:", curated_path)
print("Target Redshift Database:", redshift_db)
print("Target Redshift Table:", users_table)
print("Redshift Temp Dir:", redshift_tmp_dir)

# Step 1: Read CURATED Parquet data from S3
print("Step 1: Reading CURATED Parquet data")
df = spark.read.parquet(curated_path)

print("Read completed")
print("Total records in CURATED:", df.count())
df.printSchema()
df.show(5, False)

# Step 2: Normalize schema for Redshift
print("Step 2: Normalizing schema for Redshift")
df_rs = (
    df
    .withColumn("signup_date", F.col("signup_date").cast("date"))
    .withColumn("is_active", F.col("is_active").cast("boolean"))
    .select("user_id", "username", "email", "city", "signup_date", "is_active")
)

print("Normalized schema:")
df_rs.printSchema()
df_rs.show(5, False)

# Convert DataFrame to DynamicFrame (required for Glue Redshift writer)
print("Step 3: Converting DataFrame to DynamicFrame")
dyf = DynamicFrame.fromDF(df_rs, glueContext, "dyf_redshift")
print("DynamicFrame conversion completed")

# Step 4: Create table in Redshift (preactions)
print("Step 4: Preparing Redshift CREATE TABLE statement")

create_sql = f"""
CREATE TABLE IF NOT EXISTS {users_table} (
  user_id BIGINT,
  username VARCHAR(100),
  email VARCHAR(150),
  city VARCHAR(100),
  signup_date DATE,
  is_active BOOLEAN
);
"""

print("CREATE TABLE SQL:")
print(create_sql)

# Step 5: Load data into Redshift
print("Step 5: Writing data into Redshift (using Glue Connection via from_jdbc_conf)")

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dyf,
    catalog_connection="Redshift connection",   # must match Glue Connection name exactly
    connection_options={
        "database": redshift_db,
        "dbtable": users_table,
        "preactions": create_sql,
        "redshiftTmpDir": redshift_tmp_dir
    },
    transformation_ctx="redshift_write"
)

print("Redshift write completed")

# Commit the Glue job
job.commit()
print("Glue Job Completed: Script 4")
