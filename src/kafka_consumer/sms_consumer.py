#!/usr/bin/env python3
"""
SMS Kafka Consumer
Consumes SMS messages from Kafka, parses them using SMS parser,
and sends structured data to Elasticsearch
"""

import json
import logging
import signal
import sys
import os
import time
from datetime import datetime
from typing import Dict, List, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from threading import Thread, Event

# Add parent directories to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from config.kafka_config import KAFKA_CONFIG
from config.elasticsearch_config import ELASTICSEARCH_CONFIG
from sms_parser_engine.sms_parser import SMSParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('../../logs/sms_consumer.log')
    ]
)
logger = logging.getLogger(__name__)

class SMSKafkaConsumer:
    def __init__(self):
        """Initialize Kafka consumer with SMS parser"""
        self.consumer = None
        self.sms_parser = SMSParser()
        self.running = False
        self.stop_event = Event()
        
        # Statistics
        self.stats = {
            'messages_consumed': 0,
            'messages_parsed': 0,
            'messages_failed': 0,
            'start_time': None,
            'last_message_time': None
        }
        
        # Initialize consumer
        self._initialize_consumer()
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _initialize_consumer(self):
        """Initialize Kafka consumer with configuration"""
        try:
            self.consumer = KafkaConsumer(
                KAFKA_CONFIG['topic_name'],
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                group_id=KAFKA_CONFIG['consumer_group'],
                # Deserialization
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                # Consumer settings
                auto_offset_reset='earliest',  # Start from beginning if no offset
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                # Performance settings
                fetch_min_bytes=1,
                fetch_max_wait_ms=500,
                max_poll_records=500
            )
            
            logger.info(f"✅ Kafka Consumer initialized successfully")
            logger.info(f"📡 Subscribed to topic: {KAFKA_CONFIG['topic_name']}")
            logger.info(f"👥 Consumer group: {KAFKA_CONFIG['consumer_group']}")
            
            # Test connection
            partitions = self.consumer.partitions_for_topic(KAFKA_CONFIG['topic_name'])
            if partitions:
                logger.info(f"🔗 Found {len(partitions)} partitions for topic")
            else:
                logger.warning(f"⚠️ No partitions found for topic {KAFKA_CONFIG['topic_name']}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka Consumer: {str(e)}")
            raise
    
    def _signal_handler(self, signum, frame):
        """Handle graceful shutdown signals"""
        logger.info(f"🛑 Received signal {signum}, initiating graceful shutdown...")
        self.stop()
    
    def start_consuming(self, batch_size: int = 100, timeout_ms: int = 1000):
        """
        Start consuming SMS messages from Kafka
        
        Args:
            batch_size (int): Number of messages to process in batch
            timeout_ms (int): Consumer poll timeout in milliseconds
        """
        if self.running:
            logger.warning("⚠️ Consumer is already running")
            return
        
        self.running = True
        self.stats['start_time'] = datetime.now()
        
        logger.info("🚀 Starting SMS consumption...")
        logger.info(f"📊 Batch size: {batch_size}, Timeout: {timeout_ms}ms")
        
        try:
            message_buffer = []
            
            while self.running and not self.stop_event.is_set():
                try:
                    # Poll for messages
                    message_batch = self.consumer.poll(
                        timeout_ms=timeout_ms,
                        max_records=batch_size
                    )
                    
                    if not message_batch:
                        # No messages received, continue polling
                        continue
                    
                    # Process messages from all partitions
                    for topic_partition, messages in message_batch.items():
                        logger.info(f"📥 Received {len(messages)} messages from partition {topic_partition.partition}")
                        
                        for message in messages:
                            try:
                                # Process individual message
                                self._process_message(message)
                                self.stats['messages_consumed'] += 1
                                
                                # Update last message time
                                self.stats['last_message_time'] = datetime.now()
                                
                                # Log progress every 10 messages
                                if self.stats['messages_consumed'] % 10 == 0:
                                    self._log_progress()
                                
                            except Exception as e:
                                self.stats['messages_failed'] += 1
                                logger.error(f"❌ Error processing message: {str(e)}")
                                logger.error(f"🔍 Message content: {message.value}")
                    
                    # Commit offsets after successful batch processing
                    self.consumer.commit()
                    
                except KafkaError as e:
                    logger.error(f"❌ Kafka error during consumption: {str(e)}")
                    time.sleep(5)  # Wait before retrying
                    
                except Exception as e:
                    logger.error(f"❌ Unexpected error during consumption: {str(e)}")
                    time.sleep(1)
        
        except KeyboardInterrupt:
            logger.info("🛑 Consumer stopped by user")
        
        finally:
            self._cleanup()
    
    def _process_message(self, message):
        """
        Process individual SMS message
        
        Args:
            message: Kafka message object
        """
        try:
            # Extract message data
            sms_data = message.value
            partition = message.partition
            offset = message.offset
            timestamp = message.timestamp
            
            logger.debug(f"📨 Processing message from partition {partition}, offset {offset}")
            
            # Validate message structure
            if not isinstance(sms_data, dict):
                raise ValueError(f"Invalid message format: expected dict, got {type(sms_data)}")
            
            required_fields = ['phone', 'body']
            for field in required_fields:
                if field not in sms_data:
                    raise ValueError(f"Missing required field: {field}")
            
            # Parse SMS using your existing parser
            parsed_data = self.sms_parser.parse_sms(sms_data)
            
            if parsed_data:
                # Add Kafka metadata to parsed data
                enriched_data = {
                    **parsed_data,
                    'kafka_metadata': {
                        'topic': message.topic,
                        'partition': partition,
                        'offset': offset,
                        'kafka_timestamp': timestamp,
                        'consumer_processed_at': datetime.now().isoformat()
                    }
                }
                
                # Send to next stage (this will be Elasticsearch client)
                self._send_to_elasticsearch(enriched_data)
                
                self.stats['messages_parsed'] += 1
                
                logger.info(f"✅ Successfully processed SMS:")
                logger.info(f"   📱 Phone: {parsed_data.get('phone', 'N/A')}")
                logger.info(f"   💰 Amount: {parsed_data.get('amount', 'N/A')} OMR")
                logger.info(f"   🏪 Vendor: {parsed_data.get('vendor', 'N/A')}")
                logger.info(f"   📊 Partition: {partition}, Offset: {offset}")
                
            else:
                logger.warning(f"⚠️ Failed to parse SMS message:")
                logger.warning(f"   📱 Phone: {sms_data.get('phone', 'N/A')}")
                logger.warning(f"   📝 Body: {sms_data.get('body', 'N/A')[:50]}...")
                self.stats['messages_failed'] += 1
        
        except Exception as e:
            logger.error(f"❌ Error processing message: {str(e)}")
            self.stats['messages_failed'] += 1
            raise
    
    def _send_to_elasticsearch(self, parsed_data: Dict):
        """
        Send parsed SMS data to Elasticsearch
        This is a placeholder - will be implemented with Elasticsearch client
        
        Args:
            parsed_data (Dict): Parsed and enriched SMS data
        """
        try:
            # TODO: Implement Elasticsearch client integration
            # For now, just log the data that would be sent
            
            logger.info(f"📤 Would send to Elasticsearch:")
            logger.info(f"   Index: {ELASTICSEARCH_CONFIG['index_name']}")
            logger.info(f"   Document: {json.dumps(parsed_data, indent=2)}")
            
            # Simulate processing time
            time.sleep(0.1)
            
            # TODO: Replace with actual Elasticsearch client call
            # es_client.index_document(parsed_data)
            
        except Exception as e:
            logger.error(f"❌ Error sending to Elasticsearch: {str(e)}")
            raise
    
    def _log_progress(self):
        """Log consumption progress and statistics"""
        runtime = (datetime.now() - self.stats['start_time']).total_seconds()
        rate = self.stats['messages_consumed'] / runtime if runtime > 0 else 0
        
        logger.info(f"📊 Progress Update:")
        logger.info(f"   📥 Consumed: {self.stats['messages_consumed']}")
        logger.info(f"   ✅ Parsed: {self.stats['messages_parsed']}")
        logger.info(f"   ❌ Failed: {self.stats['messages_failed']}")
        logger.info(f"   ⚡ Rate: {rate:.2f} msg/sec")
        logger.info(f"   ⏱️ Runtime: {runtime:.1f}s")
    
    def stop(self):
        """Stop the consumer gracefully"""
        logger.info("🛑 Stopping SMS consumer...")
        self.running = False
        self.stop_event.set()
    
    def _cleanup(self):
        """Cleanup resources and log final statistics"""
        logger.info("🧹 Cleaning up consumer resources...")
        
        try:
            if self.consumer:
                self.consumer.close()
                logger.info("🔒 Kafka consumer closed successfully")
        except Exception as e:
            logger.error(f"❌ Error closing consumer: {str(e)}")
        
        # Log final statistics
        if self.stats['start_time']:
            total_runtime = (datetime.now() - self.stats['start_time']).total_seconds()
            avg_rate = self.stats['messages_consumed'] / total_runtime if total_runtime > 0 else 0
            
            logger.info("📊 Final Statistics:")
            logger.info(f"   📥 Total consumed: {self.stats['messages_consumed']}")
            logger.info(f"   ✅ Successfully parsed: {self.stats['messages_parsed']}")
            logger.info(f"   ❌ Failed to parse: {self.stats['messages_failed']}")
            logger.info(f"   ⏱️ Total runtime: {total_runtime:.1f}s")
            logger.info(f"   ⚡ Average rate: {avg_rate:.2f} msg/sec")
            
            if self.stats['messages_consumed'] > 0:
                success_rate = (self.stats['messages_parsed'] / self.stats['messages_consumed']) * 100
                logger.info(f"   📈 Success rate: {success_rate:.1f}%")

class ConsumerManager:
    """Manager class for running consumer with different modes"""
    
    def __init__(self):
        self.consumer = SMSKafkaConsumer()
    
    def run_continuous(self, batch_size: int = 100):
        """Run consumer continuously"""
        logger.info("🔄 Starting continuous consumption mode...")
        self.consumer.start_consuming(batch_size=batch_size)
    
    def run_batch(self, max_messages: int = 1000, timeout_seconds: int = 30):
        """Run consumer for a specific number of messages or timeout"""
        logger.info(f"📦 Starting batch consumption mode: max {max_messages} messages, {timeout_seconds}s timeout")
        
        start_time = datetime.now()
        initial_count = self.consumer.stats['messages_consumed']
        
        # Start consumer in separate thread
        consumer_thread = Thread(target=self.consumer.start_consuming)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        try:
            while True:
                current_time = datetime.now()
                elapsed = (current_time - start_time).total_seconds()
                messages_processed = self.consumer.stats['messages_consumed'] - initial_count
                
                # Check termination conditions
                if messages_processed >= max_messages:
                    logger.info(f"✅ Reached maximum messages: {messages_processed}")
                    break
                
                if elapsed >= timeout_seconds:
                    logger.info(f"⏰ Reached timeout: {elapsed:.1f}s")
                    break
                
                # Check if consumer is still running
                if not self.consumer.running:
                    logger.info("🛑 Consumer stopped")
                    break
                
                time.sleep(1)  # Check every second
        
        finally:
            self.consumer.stop()
            if consumer_thread.is_alive():
                consumer_thread.join(timeout=5)

def main():
    """Main function for testing the SMS consumer"""
    
    print("\n" + "="*60)
    print("🚀 SMS Kafka Consumer Test")
    print("="*60)
    
    try:
        # Choose consumption mode
        print("\nSelect consumption mode:")
        print("1. Continuous (runs until stopped)")
        print("2. Batch (process limited messages)")
        print("3. Test mode (process 5 messages)")
        
        choice = input("Enter choice (1-3): ").strip()
        
        manager = ConsumerManager()
        
        if choice == "1":
            print("\n🔄 Starting continuous mode... (Press Ctrl+C to stop)")
            manager.run_continuous(batch_size=50)
            
        elif choice == "2":
            max_msg = int(input("Max messages to process: ") or "100")
            timeout = int(input("Timeout in seconds: ") or "60")
            print(f"\n📦 Starting batch mode: {max_msg} messages, {timeout}s timeout")
            manager.run_batch(max_messages=max_msg, timeout_seconds=timeout)
            
        elif choice == "3":
            print("\n🧪 Starting test mode: processing 5 messages...")
            manager.run_batch(max_messages=5, timeout_seconds=30)
            
        else:
            print("❌ Invalid choice")
            return
    
    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped by user")
    except Exception as e:
        logger.error(f"❌ Consumer error: {str(e)}")
    
    print("\n✅ Consumer test completed!")

if __name__ == "__main__":
    main()