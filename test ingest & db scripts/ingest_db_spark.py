from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from glob import glob
from datetime import datetime, timedelta
import pyspark.sql.types as T 
from dotenv import load_dotenv
import os

load_dotenv()

pg_host = os.getenv("PGHOST")
pg_user = os.getenv("PGUSER")
pg_port = os.getenv("PGPORT")
pg_database = os.getenv("PGDATABASE")
pg_password = os.getenv("PGPASSWORD")

spark = SparkSession.builder \
    .appName("Ingest Trips Data") \
    .config("spark.driver.extraClassPath", "/usr/share/java/postgresql.jar") \
    .getOrCreate()


# Load CSV files into a DataFrame
trips_csv = glob("./db_ingest/*.csv")

schema = T.StructType([
    T.StructField("TRIPDURATION", T.IntegerType(), True),
    T.StructField("STARTTIME", T.TimestampType(), True),
    T.StructField("STOPTIME", T.TimestampType(), True),
    T.StructField("START_STATION_ID", T.IntegerType(), True),
    T.StructField("START_STATION_NAME", T.StringType(), True),
    T.StructField("START_STATION_LATITUDE", T.FloatType(), True),
    T.StructField("START_STATION_LONGITUDE", T.FloatType(), True),
    T.StructField("END_STATION_ID", T.IntegerType(), True),
    T.StructField("END_STATION_NAME", T.StringType(), True),
    T.StructField("END_STATION_LATITUDE", T.FloatType(), True),
    T.StructField("END_STATION_LONGITUDE", T.FloatType(), True),
    T.StructField("BIKEID", T.IntegerType(), True),
    T.StructField("MEMBERSHIP_TYPE", T.StringType(), True),
    T.StructField("USERTYPE", T.StringType(), True),
    T.StructField("BIRTH_YEAR", T.FloatType(), True),
    T.StructField("GENDER", T.IntegerType(), True),
])

trips_df = spark.read.csv(trips_csv, header=True, schema=schema)

# Convert string columns to timestamp and date types
trips_df = trips_df \
    .withColumn("STARTTIME", col("STARTTIME").cast("timestamp")) \
    .withColumn("STOPTIME", col("STOPTIME").cast("timestamp"))

# Add additional columns
trips_df = trips_df \
    .withColumn("PRE_REGISTRATION_STATUS", col("STARTTIME").isNotNull()) \
    .withColumn("REGISTRATION_DATE", (col("STARTTIME").cast("date") - timedelta(days=2)))

# 5% of dataset to be stored in db
trips_df = trips_df.sample(withReplacement=False, fraction=0.02, seed=42)
# Write DataFrame to database
# Define database connection parameters
db_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

# Define database URL
db_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"

# Write DataFrame to database table
trips_df.write \
    .mode("overwrite") \
    .jdbc(url=db_url, table="registered_trips", properties=db_properties)

# trips_df.write \
#     .mode("overwrite") \
#     .option("numPartitions", 20) \
#     .jdbc(url=db_url, table="registered_trips", properties=db_properties)

# batch_size = 10000

# trips_df.write \
#     .mode("overwrite") \
#     .option("batchsize", batch_size) \
#     .jdbc(url=db_url, table="registered_trips", properties=db_properties)



# Stop SparkSession
spark.stop()

# command to run
# spark-submit --jars /usr/share/java/postgresql.jar ingest_db_spark.py

