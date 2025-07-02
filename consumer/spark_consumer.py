# src/consumer/spark_consumer.py
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
from config.config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkEarthquakeConsumer:
    def __init__(self):
        self.config = Config()
        self.setup_spark_session()
        self.setup_schema()
    
    def setup_spark_session(self):
        """Setup Spark session with necessary configurations"""
        self.spark = SparkSession.builder \
        .appName("KafkaStructuredStreaming") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config("spark.hadoop.hadoop.security.authentication", "simple") \
        .config("spark.hadoop.hadoop.native.io", "false") \
        .getOrCreate()


        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
    
    def setup_schema(self):
        """Define schema for earthquake events"""
        self.earthquake_schema = StructType([
            StructField("id", StringType(), True),
            StructField("magnitude", DoubleType(), True),
            StructField("place", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("depth", DoubleType(), True),
            StructField("mag_type", StringType(), True),
            StructField("nst", IntegerType(), True),
            StructField("gap", DoubleType(), True),
            StructField("dmin", DoubleType(), True),
            StructField("rms", DoubleType(), True),
            StructField("net", StringType(), True),
            StructField("updated", StringType(), True),
            StructField("tz", StringType(), True),
            StructField("url", StringType(), True),
            StructField("detail", StringType(), True),
            StructField("felt", IntegerType(), True),
            StructField("cdi", DoubleType(), True),
            StructField("mmi", DoubleType(), True),
            StructField("alert", StringType(), True),
            StructField("status", StringType(), True),
            StructField("tsunami", IntegerType(), True),
            StructField("sig", IntegerType(), True),
            StructField("ids", StringType(), True),
            StructField("sources", StringType(), True),
            StructField("types", StringType(), True),
            StructField("event_type", StringType(), True),
        ])
    
    def create_kafka_stream(self):
        """Create Kafka streaming DataFrame"""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", 'localhost:29092') \
            .option("subscribe", self.config.KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()
    
    def process_earthquake_stream(self, kafka_df):
        """Process earthquake stream and apply transformations"""
        # Parse JSON from Kafka value
        parsed_df = kafka_df.select(
            col("key").cast("string").alias("event_key"),
            from_json(col("value").cast("string"), self.earthquake_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("event_key", "data.*", "kafka_timestamp")
        
        # Apply transformations
        processed_df = parsed_df.withColumn(
            "event_datetime", 
            from_unixtime(col("timestamp") / 1000).cast("timestamp")
        ).withColumn(
            "magnitude_category",
            when(col("magnitude") < 2.0, "Micro")
            .when(col("magnitude") < 4.0, "Minor")
            .when(col("magnitude") < 5.0, "Light")
            .when(col("magnitude") < 6.0, "Moderate")
            .when(col("magnitude") < 7.0, "Strong")
            .when(col("magnitude") < 8.0, "Major")
            .otherwise("Great")
        ).withColumn(
            "depth_category",
            when(col("depth") < 70, "Shallow")
            .when(col("depth") < 300, "Intermediate")
            .otherwise("Deep")
        ).withColumn(
            "processed_at",
            current_timestamp()
        )
        
        return processed_df
    
    def write_to_postgres(self, df, epoch_id):
        """Write batch to PostgreSQL"""
        try:
            # PostgreSQL connection properties
            postgres_properties = {
                "user": self.config.DB_USER,
                "password": self.config.DB_PASSWORD,
                "driver": "org.postgresql.Driver"
            }
            
            postgres_url = f"jdbc:postgresql://{self.config.DB_HOST}:{self.config.DB_PORT}/{self.config.DB_NAME}"
            
            # Write to PostgreSQL
            df.write \
                .mode("append") \
                .jdbc(postgres_url, "earthquakes", properties=postgres_properties)
            
            logger.info(f"Batch {epoch_id}: Written {df.count()} records to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Error writing batch {epoch_id} to PostgreSQL: {e}")
    
    def start_streaming(self):
        """Start the streaming process"""
        logger.info("Starting earthquake stream processing...")
        
        try:
            # Create Kafka stream
            kafka_df = self.create_kafka_stream()
            
            # Process stream
            processed_df = self.process_earthquake_stream(kafka_df)
            
            # Write stream to PostgreSQL
            query = processed_df.writeStream \
                .outputMode("append") \
                .foreachBatch(self.write_to_postgres) \
                .trigger(processingTime='30 seconds') \
                .start()
            
            logger.info("Streaming query started successfully")
            
            # Wait for termination
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in streaming process: {e}")
            raise
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()

if __name__ == "__main__":
    consumer = SparkEarthquakeConsumer()
    try:
        consumer.start_streaming()
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
        consumer.stop()