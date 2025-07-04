# Real-time GIS Data Engineering Pipeline

This project demonstrates a complete real-time GIS data pipeline using:
- Python for data processing
- Apache Kafka for message streaming
- Schema Registry with Protobuf
- Apache Spark Streaming for real-time processing
- PostgreSQL for data storage
- Interactive dashboard for visualization

## Quick Start

1. Prerequisites:
   - Docker and Docker Compose
   - Python 3.8+
   - Java 8/11 (for Kafka/Spark)

2. Setup:
   ```bash
   # Clone or extract the project
   cd gis-data-pipeline
   
   # Copy environment file
   cp .env.example .env
   
   # Make scripts executable
   chmod +x deploy.sh scripts/*.sh
   
   # Deploy the pipeline
   ./deploy.sh

# Start infrastructure
docker-compose up -d

# Wait for services (30 seconds)
sleep 30

# Setup database
python src/database/db_setup.py

# Start producer (in terminal 1)
python src/producer/earthquake_producer.py

# Start consumer (in terminal 2)
python src/consumer/spark_consumer.py

# Start dashboard (in terminal 3)
python src/visualization/dashboard.py

Access Dashboard:
Open http://localhost:8050 in your browser
