import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

print("Starting Glue Job: Script 3 - CURATED Parquet → Glue Data Catalog Table")

# Read job parameters passed from Glue Console
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "CURATED_PATH",
    "DB_NAME",
    "TABLE_NAME"
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue Job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

curated_path = args["CURATED_PATH"]      # S3 CURATED layer path
db_name = args["DB_NAME"]               # Glue Catalog database name
table_name = args["TABLE_NAME"]         # Glue Catalog table name

print("Input CURATED Path:", curated_path)
print("Glue Database Name:", db_name)
print("Glue Table Name:", table_name)

# Step 1: Read CURATED Parquet data from S3
print("Step 1: Reading Parquet data from CURATED layer")

df = spark.read.parquet(curated_path)

print("Read completed")
print("Total records in CURATED:", df.count())
df.printSchema()
df.show(5, False)

# Step 2: Convert Spark DataFrame to Glue DynamicFrame
print("Step 2: Converting DataFrame to DynamicFrame (required for Glue Sink)")

dyf_curated = DynamicFrame.fromDF(df, glueContext, "dyf_curated")

print("DynamicFrame conversion completed")

# Step 3: Create Glue Sink to update Data Catalog
print("Step 3: Creating Glue Sink (this will update Glue Catalog table + partitions)")

sink = glueContext.getSink(
    path=curated_path,
    connection_type="s3",
    enableUpdateCatalog=True,             # Automatically create/update table
    updateBehavior="UPDATE_IN_DATABASE",  # Update schema in Glue Catalog
    partitionKeys=["signup_date"],        # Register partitions
    transformation_ctx="sink_curated"
)

print("Glue Sink created successfully")

# Step 4: Set Glue Catalog Database and Table Name
print("Step 4: Setting Catalog Database and Table Name")

sink.setCatalogInfo(
    catalogDatabase=db_name,
    catalogTableName=table_name
)

print("Catalog info set successfully")

# Step 5: Write using Sink (Catalog will be updated)
print("Step 5: Writing frame and updating Glue Data Catalog")

sink.setFormat("glueparquet")
sink.writeFrame(dyf_curated)

print("Glue Catalog table updated successfully")
print("Partitions should now be available in Glue Data Catalog")

# Commit the Glue job
job.commit()
print("Glue Job Completed: Script 3")
