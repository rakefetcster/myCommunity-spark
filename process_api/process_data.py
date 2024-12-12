from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from kafka import KafkaProducer
import json
from pyspark.sql import functions as F

# Initialize Spark session for structured streaming
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("KafkaMultipleTopics2") \
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

# Kafka and S3 configuration
KAFKA_TOPIC = 'my-community'  # Original topic for consuming messages
PROCESSED_KAFKA_TOPIC = 'my-community-res'  # Topic to send processed messages back
KAFKA_BROKER = 'course-kafka:9092'
S3_BUCKET = 'my-community/files'


class KafkaSparkConsumerProducer:
    def __init__(self, topic, processed_topic, kafka_server='localhost:9092', s3_bucket='my-community'):
        self.topic = topic
        self.processed_topic = processed_topic  # Topic to write processed data back
        self.kafka_server = kafka_server
        self.s3_bucket = s3_bucket
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def consume_stream(self):
        """Consume Kafka messages as a stream and process them."""
        # Read the Kafka stream
        message_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_server) \
            .option("subscribe", self.topic) \
            .load()

        # The Kafka message values are in bytes, so decode them to strings
        decoded_df = message_df.selectExpr("CAST(value AS STRING) AS message")

        # Process the incoming stream of messages
        processed_df = decoded_df.select("message") \
            .writeStream \
            .foreachBatch(self.process_batch) \
            .start()

        processed_df.awaitTermination()

    def process_batch(self, batch_df, batch_id):
        """Process each batch of data."""
        # Convert each batch DataFrame to JSON format and process it
        batch_df.collect()  # Collect batch data (this is where the processing happens)

        for row in batch_df.collect():
            message = row['message']
            try:
                # Check if the message is a JSON or just a string (file path)
                data = json.loads(message)
                if 'additional_info' in data and 'api_data' in data:
                    # Process the JSON message containing both `api_data` and `additional_info`
                    self.process_json_message(data)
                else:
                    # If the message is just a string, it's the file path
                    self.process_file_message(message)
            except json.JSONDecodeError:
                # If it's not JSON, treat it as a file path string
                self.process_file_message(message)

    def process_json_message(self, data):
        """Process the JSON message, save `api_data` as a Parquet file in S3, process it, and send to Kafka."""
        additional_info = data.get('additional_info')  # This will be used as the file name
        api_data = data.get('api_data')  # This is the actual data to be processed

        # Convert the `api_data` to a Spark DataFrame
        api_df = self.convert_to_spark_dataframe(api_data)

        # Save the `api_data` as a Parquet file in S3
        s3_path = f"s3a://{self.s3_bucket}/{additional_info}.parquet"
        self.save_parquet_to_s3(api_df, s3_path)

        # Read the saved Parquet file back from S3 and process it
        # processed_df = spark.read.parquet(s3_path)
        processed_api_data = self.process_spark_data(api_df)

        # Prepare the processed message to send back to Kafka
        result_data = {
            "api_data": processed_api_data.toJSON().collect(),
            "additional_info": additional_info
        }

        # Send the processed result back to the new Kafka topic (processed_topic)
        self.producer.send(self.processed_topic, value=result_data)
        self.producer.flush()

    def process_file_message(self, file_path):
        """Process the file message (file path) from S3 and apply transformations."""
        print(f"Processing file from S3: {file_path}")
        
        # Read the Parquet file from S3
        df = spark.read.parquet(file_path)
        
        # Process the Spark DataFrame (apply any transformations here)
        processed_df = self.process_spark_data(df)
        
        # Send the processed result back to the new Kafka topic (processed_topic)
        result_data = {
            "processed_info": f"Data from {file_path} processed successfully",
            "processed_data": processed_df.toJSON().collect()
        }

        # Send the processed result back to Kafka
        self.producer.send(self.processed_topic, value=result_data)
        self.producer.flush()

    def process_spark_data(self, df):
        """Perform data processing using Spark DataFrame transformations."""
        # Example transformation: Add a new column to the DataFrame
        # processed_df = df.withColumn('processed_column', lit('processed_value'))
        # Exploding the 'data' array to flatten the structure
        df_exploded = df.select(F.explode(df.data).alias("business_data"))

        # Flattening the nested JSON fields and selecting the relevant fields
        df_flat = df_exploded.select(
            "business_data.name",
            "business_data.full_address",
            "business_data.website",
            "business_data.reviews_link",
            "business_data.type",
            "business_data.place_link",
            "business_data.rating",
            "business_data.latitude",
            "business_data.longitude",
        )

        # Sorting by rating (descending order) to get the top 5 highest-rated restaurants
        df_top_rated = df_flat.orderBy(F.desc("rating")).limit(5)

        # Show the top 5 highest-rated restaurants with the selected columns
        df_top_rated.show(truncate=False)
        return df_top_rated

    def save_parquet_to_s3(self, dataframe, s3_path):
        """Save the DataFrame as a Parquet file in S3."""
        print(f"Saving Parquet file to S3: {s3_path}")
        
        # Write the DataFrame to the specified S3 path as Parquet
        dataframe.write.parquet(s3_path, mode='overwrite')
        print(f"Parquet data saved to S3: {s3_path}")

    def convert_to_spark_dataframe(self, api_data):
        """Convert a Python dictionary (api_data) to a Spark DataFrame."""
        # If api_data is a dictionary, convert it to a DataFrame
        if isinstance(api_data, dict):
            return spark.createDataFrame([api_data])  # Convert single dict to DataFrame
        else:
            # If api_data is already in a format that can be used for DataFrame creation (e.g., list of dicts)
            return spark.createDataFrame(api_data)  # You may need to adjust this based on the actual structure

    def close(self):
        """Close the Kafka producer connection."""
        self.producer.close()
        print("Kafka producer connection closed.")


def main():
    # Initialize the consumer-producer with the original topic and the processed topic
    consumer_producer = KafkaSparkConsumerProducer(
        topic=KAFKA_TOPIC,
        processed_topic=PROCESSED_KAFKA_TOPIC,
        kafka_server=KAFKA_BROKER,
        s3_bucket=S3_BUCKET
    )

    # Start consuming and processing messages in streaming mode
    consumer_producer.consume_stream()

    # Close the producer connection when done
    consumer_producer.close()


if __name__ == "__main__":
    main()
