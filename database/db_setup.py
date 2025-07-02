# src/database/db_setup.py
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
from config.config import Config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseSetup:
    def __init__(self):
        self.config = Config()
        
    def create_connection(self):
        """Create database connection"""
        try:
            conn = psycopg2.connect(
                host=self.config.DB_HOST,
                port=self.config.DB_PORT,
                database=self.config.DB_NAME,
                user=self.config.DB_USER,
                password=self.config.DB_PASSWORD
            )
            return conn
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise
    
    def create_tables(self):
        """Create necessary tables"""
        conn = self.create_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        try:
            cursor = conn.cursor()
            
            # Create earthquakes table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS earthquakes (
                id VARCHAR(255) PRIMARY KEY,
                magnitude DOUBLE PRECISION,
                place TEXT,
                timestamp BIGINT,
                longitude DOUBLE PRECISION,
                latitude DOUBLE PRECISION,
                depth DOUBLE PRECISION,
                mag_type VARCHAR(10),
                nst INTEGER,
                gap DOUBLE PRECISION,
                dmin DOUBLE PRECISION,
                rms DOUBLE PRECISION,
                net VARCHAR(10),
                updated VARCHAR(50),
                tz VARCHAR(10),
                url TEXT,
                detail TEXT,
                felt INTEGER,
                cdi DOUBLE PRECISION,
                mmi DOUBLE PRECISION,
                alert VARCHAR(20),
                status VARCHAR(20),
                tsunami INTEGER,
                sig INTEGER,
                ids TEXT,
                sources TEXT,
                types TEXT,
                event_type VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            cursor.execute(create_table_query)
            
            # Create index on timestamp for better query performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_earthquakes_timestamp 
                ON earthquakes(timestamp);
            """)
            
            # Create index on magnitude
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_earthquakes_magnitude 
                ON earthquakes(magnitude);
            """)
            
            # Create spatial index on coordinates
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_earthquakes_location 
                ON earthquakes(longitude, latitude);
            """)
            
            logger.info("Database tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
        finally:
            conn.close()

if __name__ == "__main__":
    db_setup = DatabaseSetup()
    db_setup.create_tables()