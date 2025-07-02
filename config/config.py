# config/config.py
import os
from dataclasses import dataclass

@dataclass
class Config:
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
    KAFKA_TOPIC = 'earthquake-events'
    
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', "5432")
    DB_NAME = os.getenv('DB_NAME', 'postgres')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '12345')
    
    # API Configuration
    USGS_API_URL = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
    FETCH_INTERVAL = 60  # seconds
    
    # Spark Configuration
    SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://localhost:7077')
    CHECKPOINT_LOCATION = '/tmp/spark-checkpoint'