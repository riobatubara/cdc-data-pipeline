from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

# 1. Spin up the localized Spark session engine with fallback parameters
spark = SparkSession.builder \
    .appName("Postgres-CDC-Flatten-To-MinIO") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# 2. Bind the streaming extraction thread to Kafka 
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "cdc_kafka:9092") \
    .option("subscribe", "cdc.public.orders") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Define the structural schema blueprint of Debezium's "after" object block
order_fields_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("amount", StringType(), True), 
    StructField("status", StringType(), True),
    StructField("updated_at", LongType(), True)
])

# Outer wrapper envelope configuration schema
debezium_envelope_schema = StructType([
    StructField("payload", StructType([
        StructField("after", order_fields_schema, True),
        StructField("op", StringType(), True)
    ]), True)
])

# 4. Parse the raw string stream payload value data
string_json_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_body")

parsed_df = string_json_df.withColumn("data", from_json(col("json_body"), debezium_envelope_schema)) \
    .select("data.payload.after.*", "data.payload.op") \
    .filter("op != 'd'") 

# 5. Transform and decode the base64 DECIMAL value back into a readable Float number
final_flattened_df = parsed_df.withColumn(
    "amount", 
    expr("cast(unbase64(amount) as string)").cast("decimal(10,2)")
)

# 6. Push the streaming data straight into MinIO via Delta Lake formats
checkpoint_directory = "s3a://raw-zone/checkpoints/orders_cdc"
output_lakehouse_directory = "s3a://raw-zone/delta/orders"

query = final_flattened_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_directory) \
    .start(output_lakehouse_directory)

query.awaitTermination()
