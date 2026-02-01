import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

print("Starting Glue Job: Script 2 - PROCESSED Parquet → CURATED Parquet (partitioned)")

# Read job parameters passed from Glue Console
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "PROCESSED_PATH",
    "CURATED_PATH"
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue Job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

processed_path = args["PROCESSED_PATH"]  # S3 PROCESSED layer path
curated_path = args["CURATED_PATH"]      # S3 CURATED layer path

print("Input PROCESSED Path:", processed_path)
print("Output CURATED Path:", curated_path)

# Step 1: Read PROCESSED data (Parquet) from S3
print("Step 1: Reading Parquet data from PROCESSED layer")
df = spark.read.parquet(processed_path)

print("Read completed")
print("Total records in PROCESSED:", df.count())
df.printSchema()

# Step 2: Apply curated transformations 
print("Step 2: Applying curated transformations")

df_curated = (
    df
    # Data quality filter: keep only records where user_id is present
    .filter(F.col("user_id").isNotNull())

    # Data quality filter: keep only records where email looks valid
    .filter(F.col("email").contains("@"))

    # Standardize city values for analytics consistency
    .withColumn("city", F.initcap(F.trim(F.col("city"))))

    .withColumn("signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd"))

    .select("user_id", "username", "email", "city", "signup_date", "is_active")
)

print("Curated transformations completed")
print("Total records after curated filters:", df_curated.count())
df_curated.show(10, False)
df_curated.printSchema()

# Step 3: Write CURATED output as partitioned Parquet
print("Step 3: Writing CURATED Parquet partitioned by signup_date")

(df_curated
 # creates 1 parquet file per partition 
 .coalesce(1)
 .write
 .mode("overwrite")                 
 .format("parquet")
 .option("compression", "snappy")
 .partitionBy("signup_date")
 .save(curated_path)
)

print("CURATED Parquet write completed")

# Commit the Glue job
job.commit()
print("Glue Job Completed: Script 2")
