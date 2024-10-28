from kafka import KafkaConsumer
import json
import happybase
import uuid
from datetime import datetime
import time
import socket
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HBaseConnectionError(Exception):
    """Custom exception for HBase connection issues."""
    pass

class ProductAnalysisConsumer:
    def __init__(self, bootstrap_servers=['localhost:29092'], hbase_host='localhost', hbase_port=9090, max_retries=3, retry_delay=5):
        self.bootstrap_servers = bootstrap_servers
        self.hbase_host = hbase_host
        self.hbase_port = hbase_port
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.table_name = b'product_analysis'
        self.consumer = None
        self.connection = None
        self.table = None
        
        # Initialize connections
        self._initialize_connections()

    def _initialize_connections(self):
        """Initialize Kafka and HBase connections with retry logic."""
        self._init_kafka_consumer()
        self._init_hbase_connection()

    def _init_kafka_consumer(self):
        """Initialize Kafka consumer with retry logic."""
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                self.consumer = KafkaConsumer(
                    'product_analysis',
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    auto_offset_reset='earliest',
                    group_id='product_analysis_group'
                )
                logger.info("Successfully connected to Kafka")
                break
            except Exception as e:
                retry_count += 1
                if retry_count == self.max_retries:
                    raise Exception(f"Failed to connect to Kafka after {self.max_retries} attempts: {str(e)}")
                logger.warning(f"Kafka connection attempt {retry_count} failed. Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)

    def _init_hbase_connection(self):
        """Initialize HBase connection with retry logic."""
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                self.connection = happybase.Connection(
                    host=self.hbase_host,
                    port=self.hbase_port,
                    timeout=20000  # Extended timeout
                )
                self.connection.open()  # Explicitly open the connection
                self._ensure_table_exists()  # Ensure table exists
                self.table = self.connection.table(self.table_name)
                logger.info("Successfully connected to HBase")
                break
            except (socket.error, happybase.hbase.ttypes.IOError) as e:
                retry_count += 1
                if retry_count == self.max_retries:
                    raise HBaseConnectionError(f"Failed to connect to HBase after {self.max_retries} attempts: {str(e)}")
                logger.warning(f"HBase connection attempt {retry_count} failed. Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)

    def _ensure_table_exists(self):
        """Ensure HBase table exists."""
        if self.table_name not in self.connection.tables():
            logger.info(f"Table {self.table_name} does not exist. Creating table...")
            try:
                self.connection.create_table(
                    self.table_name,
                    {
                        'info': dict(),      # Column family for product information
                        'metrics': dict()    # Column family for metrics
                    }
                )
                logger.info(f"Table {self.table_name} created successfully")
            except Exception as e:
                raise HBaseConnectionError(f"Failed to create HBase table: {str(e)}")
        else:
            logger.info(f"Table {self.table_name} already exists")

    def _store_in_hbase(self, row_key, data):
        """Store data in HBase with retry logic."""
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                self.table.put(row_key, data)
                return
            except (socket.error, happybase.hbase.ttypes.IOError) as e:
                retry_count += 1
                if retry_count == self.max_retries:
                    raise HBaseConnectionError(f"Failed to store data in HBase after {self.max_retries} attempts: {str(e)}")
                logger.warning(f"HBase storage attempt {retry_count} failed. Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
                self._init_hbase_connection()  # Reinitialize HBase connection if storage fails

    def start_consuming(self):
        """Start consuming messages from Kafka and storing them in HBase."""
        logger.info("Starting consumer... Waiting for messages")
        try:
            for message in self.consumer:
                try:
                    data = message.value
                    row_key = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{str(uuid.uuid4())[:8]}".encode()
                    
                    # Prepare data for HBase insertion
                    product_data = {
                        b'info:product_id': str(data['product_id']).encode(),
                        b'info:name': str(data['product_name']).encode(),
                        b'info:brand': str(data['product_brand']).encode(),
                        b'info:categories': str(data['product_categories']).encode(),
                        b'info:manufacturer': str(data['product_manufacturer']).encode(),
                        b'metrics:total_reviews': str(data['total_reviews']).encode(),
                        b'metrics:positive_percentage': str(data['positive_percentage']).encode(),
                        b'metrics:negative_percentage': str(data['negative_percentage']).encode(),
                        b'metrics:timestamp': str(datetime.now()).encode()
                    }
                    
                    # Store data with retry mechanism
                    self._store_in_hbase(row_key, product_data)
                    logger.info(f"Stored analysis in HBase - Product: {data['product_name']}, Key: {row_key.decode()}")
                    
                except HBaseConnectionError as e:
                    logger.error(f"HBase storage error: {str(e)}")
                except KeyError as e:
                    logger.error(f"Invalid message format: {str(e)}")
                except Exception as e:
                    logger.error(f"Unexpected error processing message: {str(e)}")
                    
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        finally:
            self.close()

    def close(self):
        """Close all connections."""
        try:
            if self.consumer:
                self.consumer.close()
            if self.connection:
                self.connection.close()
            logger.info("Connections closed successfully")
        except Exception as e:
            logger.error(f"Error while closing connections: {str(e)}")

if __name__ == "__main__":
    # Configuration
    config = {
        'bootstrap_servers': ['localhost:29092'],
        'hbase_host': 'localhost',
        'hbase_port': 9090,
        'max_retries': 3,
        'retry_delay': 5
    }
    
    try:
        consumer = ProductAnalysisConsumer(**config)
        consumer.start_consuming()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        exit(1)
