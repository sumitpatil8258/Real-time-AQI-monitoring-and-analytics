import time
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import (
    col,
    from_json,
    when,
    isnan,
    round,
	avg
)
from pyspark.sql.types import *
from pyspark.sql.types import DoubleType, FloatType

# ---------------------------------------------------
# Create Spark Session
# ---------------------------------------------------
spark = (
    SparkSession.builder
    .appName("AQI Kafka Consumer")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------
# Define JSON Schema
# ---------------------------------------------------
schema = StructType([

    StructField("NAME", StringType(), True),

    StructField("HUMIDITY", DoubleType(), True),
    StructField("LIGHT", DoubleType(), True),

    StructField("NO_MAX", DoubleType(), True),
    StructField("NO_MIN", DoubleType(), True),

    StructField("NO2_MAX", DoubleType(), True),
    StructField("NO2_MIN", DoubleType(), True),

    StructField("OZONE_MAX", DoubleType(), True),
    StructField("OZONE_MIN", DoubleType(), True),

    StructField("PM10_MAX", DoubleType(), True),
    StructField("PM10_MIN", DoubleType(), True),

    StructField("PM2_MAX", DoubleType(), True),
    StructField("PM2_MIN", DoubleType(), True),

    StructField("SO2_MAX", DoubleType(), True),
    StructField("SO2_MIN", DoubleType(), True),

    StructField("CO_MAX", DoubleType(), True),
    StructField("CO_MIN", DoubleType(), True),

    StructField("CO2_MAX", DoubleType(), True),
    StructField("CO2_MIN", DoubleType(), True),

    StructField("TEMPRATURE_MAX", DoubleType(), True),
    StructField("TEMPRATURE_MIN", DoubleType(), True),

    StructField("SOUND", DoubleType(), True),

    StructField("UV_MAX", DoubleType(), True),
    StructField("UV_MIN", DoubleType(), True),

    StructField("AIR_PRESSURE", DoubleType(), True),

    StructField("LASTUPDATEDATETIME", StringType(), True),

    StructField("Lattitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True)
])

# ---------------------------------------------------
# Read Kafka Stream
# ---------------------------------------------------
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "aqi-topic")
    .option("startingOffsets", "latest")
    .load()
)

# ---------------------------------------------------
# Parse JSON
# ---------------------------------------------------
json_df = kafka_df.selectExpr("CAST(value AS STRING)")

parsed_df = (
    json_df
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)
parsed_df = (
    parsed_df.withColumn(
        "LASTUPDATEDATETIME",
        to_timestamp(
            col("LASTUPDATEDATETIME"),
            "dd-MM-yyyy HH:mm"
        )
    )
)

def write_to_mysql(batch_df, batch_id):

    if batch_df.count() == 0:
        return

    batch_df = batch_df.dropDuplicates(
        ["LASTUPDATEDATETIME", "NAME"]
    )
    # Replace NaN with NULL
    for field in batch_df.schema.fields:
        if isinstance(field.dataType, (DoubleType, FloatType)):
            batch_df = batch_df.withColumn(
                field.name,
                when(
                    col(field.name).isNull() | isnan(col(field.name)),
                    None
                ).otherwise(col(field.name))
            )
    # -------------------------
    # RAW DATA
    # -------------------------
    raw_df = batch_df

    raw_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/aqi_monitoring") \
        .option("dbtable", "raw_aqi_data") \
        .option("user", "root") \
        .option("password", "root") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
    # ---------------------------------
    # CLEANED DATAFRAME
    # ---------------------------------
    
    cleaned_df = batch_df.select(

        col("LASTUPDATEDATETIME").alias("last_updated_datetime"),

        col("NAME").alias("station_name"),

        col("Lattitude").alias("latitude"),

        col("Longitude").alias("longitude"),

        round((col("PM2_MAX") + col("PM2_MIN")) / 2, 2).alias("pm2"),

        round((col("PM10_MAX") + col("PM10_MIN")) / 2, 2).alias("pm10"),

        round((col("NO2_MAX") + col("NO2_MIN")) / 2, 2).alias("no2"),

        round((col("SO2_MAX") + col("SO2_MIN")) / 2, 2).alias("so2"),

        round((col("CO_MAX") + col("CO_MIN")) / 2, 2).alias("co"),

        round((col("TEMPRATURE_MAX") + col("TEMPRATURE_MIN")) / 2, 2).alias("temperature"),

        col("HUMIDITY").alias("humidity"),

        col("AIR_PRESSURE").alias("air_pressure")
    )
    
 
    numeric_columns = [
    	"pm2",
    	"pm10",
    	"no2",
    	"so2",
    	"co",
    	"temperature",
    	"humidity",
    	"air_pressure"
	]

    for column in numeric_columns:

        mean_value = (
            cleaned_df
            .filter((col(column).isNotNull()) & (col(column) != 0))
            .select(avg(col(column)))
            .first()[0]
    )

    	# If all values are NULL/0, use 0 as default
        if mean_value is None:
            mean_value = 0

        cleaned_df = cleaned_df.withColumn(
            column,
            when(
                col(column).isNull(),
                mean_value
            ).otherwise(col(column))
        )
    	
# ---------------------------------
# CALCULATE AIR INDEX
# ---------------------------------

    cleaned_df = cleaned_df.withColumn(
    "air_index",
    round(
        (
            col("pm2") * 0.05 +
            col("pm10") * 0.025 +
            col("no2") * 0.015 +
            col("so2") * 0.05 +
            col("co") * 0.05
        ) * 10,
        0
    ).cast("int")
)
    
    cleaned_df.show(5, truncate=False)

# ---------------------------------
# WRITE CLEANED DATA
# ---------------------------------

    cleaned_df.write \
    	.format("jdbc") \
    	.option("url", "jdbc:mysql://mysql:3306/aqi_monitoring") \
    	.option("dbtable", "aqi_data") \
    	.option("user", "root") \
    	.option("password", "root") \
    	.option("driver", "com.mysql.cj.jdbc.Driver") \
    	.mode("append") \
    	.save()

    print(f"Batch {batch_id} successfully written to both tables.")
    start_time = time.time()

    received = batch_df.count()

    if received == 0:
        return

    # Your existing processing...
    # cleaned_df.write(...)

    end_time = time.time()

    duration = end_time - start_time
    throughput = received / duration if duration > 0 else 0

    print("=" * 60)
    print(f"Batch ID            : {batch_id}")
    print(f"Records received    : {received}")
    print(f"Processing time     : {duration:.2f} seconds")
    print(f"Consumer throughput : {throughput:.2f} records/second")
# ---------------------------------------------------
# Start Streaming
# ---------------------------------------------------
query = (
    parsed_df
    .writeStream
    .outputMode("append")
    .foreachBatch(write_to_mysql)
    .start()
)

query.awaitTermination()

