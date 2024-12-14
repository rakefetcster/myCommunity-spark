import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import os

def check_file_exists_s3(path):
    """
    Comprehensive file existence check for Spark-style Parquet datasets
    """
    if not path or path == "":
        print(f"DEBUG: Empty path provided")
        return "not exist"
    
    try:
        # Configure boto3 client for MinIO
        s3_client = boto3.client(
            's3', 
            endpoint_url='http://minio:9000',
            aws_access_key_id='minioadmin', 
            aws_secret_access_key='minioadmin',
            config=boto3.session.Config(
                signature_version='s3v4', 
                connect_timeout=10, 
                read_timeout=10
            )
        )
        
        # Normalize path (remove leading/trailing spaces and quotes)
        path = path.strip().replace('"', '').replace("'", "")  # Remove quotes
        
        # Ensure path doesn't have .parquet if it's a Spark dataset
        if path.endswith('.parquet'):
            path = path[:-8]  # Remove .parquet extension
        
        bucket_name = 'my-community'
        object_key = f'files/{path}.parquet'
        
        print(f"DEBUG: Checking path: {object_key}")
        
        # Check if the directory exists
        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name, 
                Prefix=object_key,
                MaxKeys=1
            )
            
            # Check if any objects exist in the directory
            if 'Contents' in response and response['Contents']:
                print(f"DEBUG: File/Directory exists - {object_key}")
                return "exists"
            else:
                print(f"DEBUG: No objects found in - {object_key}")
                return "not exist"
        
        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"DEBUG: ClientError - Error Code: {error_code}")
            
            if error_code == '404':
                print(f"DEBUG: File/Directory not found - {object_key}")
                return "not exist"
            else:
                print(f"DEBUG: Unexpected error checking file - {e}")
                return "error"
    
    except Exception as e:
        print(f"DEBUG: Unexpected exception - {e}")
        return "error"

def read_from_kafka(spark, topic, kafka_bootstrap_servers):
    """Function to read data from Kafka topic (streaming)."""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

def process_topic1(df):
    """Transform data for topic1 by checking if the file exists."""
    # Register file_check as a UDF using the boto3-based method
    file_check_udf = F.udf(check_file_exists_s3, StringType())
    
    # Add file_status column by applying file_check UDF to 'message' column
    df_transformed = df.withColumn(
        "file_status", 
        file_check_udf(df["message"])
    )

    return df_transformed

def write_to_kafka(df, kafka_bootstrap_servers, output_topic, checkpoint_location):
    """Function to write data back to Kafka."""
    # Convert 'file_status' to a string type if needed
    df_string = df.selectExpr("CAST(file_status AS STRING) AS value")
    return df_string \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_topic) \
        .option("checkpointLocation", checkpoint_location) \
        .outputMode("append") \
        .start()

def main():
    # Determine the Spark master based on environment
    spark_master = os.getenv('SPARK_MASTER', 'local[*]')
    
    # Initialize Spark session with MinIO configurations
    spark = SparkSession.builder \
        .master(spark_master) \
        .appName("KafkaMultipleTopics") \
        .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0," +
            "org.apache.hadoop:hadoop-aws:3.3.4," +
            "com.amazonaws:aws-java-sdk-bundle:1.12.261"
        ) \
        .config("fs.s3a.access.key", "minioadmin") \
        .config("fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .config("fs.s3a.path.style.access", "true") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.s3a.metrics.enabled", "false") \
        .getOrCreate()

    # Kafka Configuration
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'course-kafka:9092')
    input_topic1 = os.getenv('INPUT_TOPIC', 'is-data-exist')
    output_topic = os.getenv('OUTPUT_TOPIC', 'is-data-exist-res')
    checkpoint_location = os.getenv('CHECKPOINT_LOCATION', 's3a://my-community/tmp')

    # Read from Kafka topic (streaming)
    df_kafka_topic1 = read_from_kafka(spark, input_topic1, kafka_bootstrap_servers)
    
    # Decode Kafka value and create message column
    df_kafka_decoded = df_kafka_topic1.select(
        F.col("value").cast("string").alias("message")
    )

    # Transform data to check file existence
    df_transformed1 = process_topic1(df_kafka_decoded)

    # Write to Kafka (as string data)
    query = write_to_kafka(df_transformed1, kafka_bootstrap_servers, output_topic, checkpoint_location)

    # Wait for termination (this keeps the stream running indefinitely)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()