from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import boto3
from botocore.exceptions import NoCredentialsError, ClientError


def read_from_kafka(spark, topic, kafka_bootstrap_servers):
    """Function to read data from Kafka topic (streaming)."""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false")\
        .load()


def check_file_exists(path):
    """Checks if a file exists on MinIO."""
    try:
        # Append .parquet to the file name if not already present
        if not path.endswith('.parquet'):
            path = path + '.parquet'
        
        # Initialize the S3 client for MinIO
        s3 = boto3.client('s3', 
                          endpoint_url="http://minio:9000",  # MinIO endpoint
                          aws_access_key_id="minioadmin",    # MinIO access key
                          aws_secret_access_key="minioadmin") # MinIO secret key
        
        # Define your bucket name
        bucket_name = "my-community"
        
        # Ensure the file is inside the "files" folder
        file_key = f"files/{path}"  # Assuming the file is in the "files" folder
        
        # Check if the file exists by using the head_object call
        print(f"Checking file in bucket: {bucket_name}, file key: {file_key}")
        try:
            s3.head_object(Bucket=bucket_name, Key=file_key)
            return True  # File exists
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False  # File does not exist
            else:
                raise e  # Reraise other exceptions

    
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except Exception as e:
        print(f"Error checking file existence: {e}")
        return False


def process_topic1(df):
    """Transform data for topic1 by checking if the file exists."""
    
    def file_check(path):
        """Checks whether a file exists in S3."""
        if path is None or path == "":
            return "not exist"
        if check_file_exists(path):
            return "exists"
        else:
            return "not exist"
    
    # Register the UDF for file check
    file_check_udf = F.udf(file_check, returnType=StringType())
    
    # Apply the UDF to the message column to check file existence
    return df.withColumn("file_status", file_check_udf(df["message"]))


def write_to_kafka(df, kafka_bootstrap_servers, output_topic, checkpoint_location):
    """Function to write data back to Kafka."""
    # Convert 'file_status' to a string type if needed
    df_string = df.selectExpr("CAST(file_status AS STRING) AS value")
    # df_string = df.selectExpr("CAST(file_status AS STRING) AS value", "CAST(file_id AS STRING) AS key")

    return df_string \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_topic) \
        .option("checkpointLocation", checkpoint_location) \
        .outputMode("append") \
        .start()


def main():
    # Initialize Spark session with MinIO configurations
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("KafkaMultipleTopics") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
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
    kafka_bootstrap_servers = "course-kafka:9092"  # Kafka Bootstrap Servers
    input_topic1 = "is-data-exist"  # Topic 1 (read)
    output_topic = "is-data-exist-res"  # Output Kafka Topic (write)
    checkpoint_location = "s3a://my-community/tmp"  # Checkpoint location (could be S3 or local)

    # Read from Kafka topic (streaming)
    df_kafka_topic1 = read_from_kafka(spark, input_topic1, kafka_bootstrap_servers)
    
    # Decode Kafka value and create message column
    df_kafka_decoded = df_kafka_topic1.select(
        F.col("value").cast("string").alias("message")
    )

    # Transform data to check file existence
    df_transformed1 = process_topic1(df_kafka_decoded)

    # Write to Kafka (as string data)
    write_to_kafka(df_transformed1, kafka_bootstrap_servers, output_topic, checkpoint_location)

    # Wait for termination (this keeps the stream running indefinitely)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
