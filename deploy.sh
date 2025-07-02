#!/bin/bash
# deploy.sh

echo "Starting GIS Data Pipeline Deployment..."

# Create project directory
mkdir -p gis-data-pipeline
cd gis-data-pipeline

# Create directory structure
mkdir -p {src/{producer,consumer,database,visualization},config,schemas,logs}

# Generate Protobuf Python files
echo "Generating Protobuf files..."
protoc --python_out=src/ schemas/earthquake.proto

# Start Docker services
echo "Starting Docker services..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30

# Setup database
echo "Setting up database..."
python src/database/db_setup.py

# Install Python dependencies
pip install -r requirements.txt

echo "Deployment completed!"
echo "Access the dashboard at: http://localhost:8050"