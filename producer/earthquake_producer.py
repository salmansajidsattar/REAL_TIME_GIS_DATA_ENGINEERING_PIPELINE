# src/producer/earthquake_producer.py
import json
import time
import requests
import logging
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import schedule
from config.config import Config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EarthquakeProducer:
    def __init__(self):
        self.config = Config()
        self.setup_kafka_producer()
        self.last_fetch_time = None
        self.admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
        self.existing_topics = self.admin_client.list_topics()
        if self.config.KAFKA_TOPIC in self.existing_topics:
            print(f"Topic '{self.config.KAFKA_TOPIC}' already exists. Skipping creation.")
        else:
            topic = NewTopic(name=self.config.KAFKA_TOPIC, num_partitions=1, replication_factor=1)
            self.admin_client.create_topics([topic])
            print(f"Topic '{self.config.KAFKA_TOPIC}' created.")
        
    def setup_kafka_producer(self):
        """Setup Kafka producer with Avro serialization"""
        try:
            # For simplicity, using JSON serialization instead of Protobuf
            # In production, you'd use Protobuf with Schema Registry
            self.producer = KafkaProducer(
                bootstrap_servers=[self.config.KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None
            )
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing Kafka producer: {e}")
            raise
    
    def fetch_earthquake_data(self):
        """Fetch earthquake data from USGS API"""
        try:
            response = requests.get(self.config.USGS_API_URL, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return data.get('features', [])
            
        except requests.RequestException as e:
            logger.error(f"Error fetching earthquake data: {e}")
            return []
    
    def transform_earthquake_event(self, feature):
        """Transform earthquake feature to our schema"""
        properties = feature.get('properties', {})
        geometry = feature.get('geometry', {})
        coordinates = geometry.get('coordinates', [0, 0, 0])
        
        return {
            'id': feature.get('id', ''),
            'magnitude': properties.get('mag', 0.0),
            'place': properties.get('place', ''),
            'timestamp': properties.get('time', 0),
            'longitude': coordinates[0] if len(coordinates) > 0 else 0.0,
            'latitude': coordinates[1] if len(coordinates) > 1 else 0.0,
            'depth': coordinates[2] if len(coordinates) > 2 else 0.0,
            'mag_type': properties.get('magType', ''),
            'nst': properties.get('nst', 0),
            'gap': properties.get('gap', 0.0),
            'dmin': properties.get('dmin', 0.0),
            'rms': properties.get('rms', 0.0),
            'net': properties.get('net', ''),
            'updated': str(properties.get('updated', '')),
            'tz': str(properties.get('tz', '')),
            'url': properties.get('url', ''),
            'detail': properties.get('detail', ''),
            'felt': properties.get('felt', 0),
            'cdi': properties.get('cdi', 0.0),
            'mmi': properties.get('mmi', 0.0),
            'alert': properties.get('alert', ''),
            'status': properties.get('status', ''),
            'tsunami': properties.get('tsunami', 0),
            'sig': properties.get('sig', 0),
            'ids': properties.get('ids', ''),
            'sources': properties.get('sources', ''),
            'types': properties.get('types', ''),
            'event_type': properties.get('type', ''),
        }
    
    def produce_earthquake_events(self):
        """Fetch and produce earthquake events to Kafka"""
        logger.info("Fetching earthquake data...")
        
        earthquake_features = self.fetch_earthquake_data()
        print(f"Fetched {len(earthquake_features)} earthquake features from USGS API")
        print("=================================================")
        
        if not earthquake_features:
            logger.warning("No earthquake data received")
            return
        
        events_produced = 0
        
        for feature in earthquake_features:
            try:
                # Transform the event
                earthquake_event = self.transform_earthquake_event(feature)
                
                # Skip if this is an old event (optional filtering)
                event_time = earthquake_event['timestamp']
                if self.last_fetch_time and event_time < self.last_fetch_time:
                    continue
                
                # Produce to Kafka
                self.producer.send(
                    self.config.KAFKA_TOPIC,
                    key=earthquake_event['id'],
                    value=earthquake_event
                )
                
                events_produced += 1
                
            except Exception as e:
                logger.error(f"Error processing earthquake event: {e}")
                continue
        
        # Flush to ensure all messages are sent
        self.producer.flush()
        
        logger.info(f"Produced {events_produced} earthquake events to Kafka")
        self.last_fetch_time = int(time.time() * 1000)  # Update last fetch time
    
    def start_scheduled_production(self):
        """Start scheduled data production"""
        logger.info(f"Starting scheduled earthquake data production every {self.config.FETCH_INTERVAL} seconds")
        
        # Initial fetch
        self.produce_earthquake_events()
        
        # Schedule regular fetches
        schedule.every(self.config.FETCH_INTERVAL).seconds.do(self.produce_earthquake_events)
        
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    def close(self):
        """Close producer"""
        if self.producer:
            self.producer.close()

if __name__ == "__main__":
    producer = EarthquakeProducer()
    try:
        producer.start_scheduled_production()
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
        producer.close()