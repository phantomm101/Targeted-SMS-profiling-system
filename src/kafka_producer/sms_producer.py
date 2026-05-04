#!/usr/bin/env python3
"""
SMS Kafka Producer
Sends SMS messages to Kafka topic for processing
Supports both CSV file input and real-time simulation
"""

import json
import csv
import time
import logging
import random
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import os

# Add parent directory to path to import configs
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from config.kafka_config import KAFKA_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SMSKafkaProducer:
    def __init__(self):
        """Initialize Kafka producer with configuration"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                # Reliability settings
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                retry_backoff_ms=1000,
                # Performance settings
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432
            )
            self.topic = KAFKA_CONFIG['topic_name']
            logger.info(f"✅ Kafka Producer initialized successfully")
            logger.info(f"📡 Target topic: {self.topic}")
            
            # Test connection
            self._test_connection()
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka Producer: {str(e)}")
            raise
    
    def _test_connection(self):
        """Test Kafka connection"""
        try:
            # Get cluster metadata to test connection
            metadata = self.producer.partitions_for(self.topic)
            if metadata is not None:
                logger.info(f"🔗 Successfully connected to Kafka cluster")
            else:
                logger.warning(f"⚠️ Topic '{self.topic}' may not exist yet")
        except Exception as e:
            logger.error(f"❌ Kafka connection test failed: {str(e)}")
    
    def send_sms(self, sms_message: Dict, phone_key: Optional[str] = None) -> bool:
        """
        Send single SMS message to Kafka
        
        Args:
            sms_message (Dict): SMS data with 'phone', 'body', 'timestamp'
            phone_key (str, optional): Use phone number as partition key
            
        Returns:
            bool: True if sent successfully, False otherwise
        """
        try:
            # Validate SMS message
            if not self._validate_sms(sms_message):
                return False
            
            # Use phone number as key for consistent partitioning
            key = phone_key or sms_message.get('phone', None)
            
            # Add metadata
            enriched_message = {
                **sms_message,
                'producer_timestamp': datetime.now().isoformat(),
                'message_id': f"{sms_message.get('phone', 'unknown')}_{int(time.time())}"
            }
            
            # Send to Kafka
            future = self.producer.send(
                topic=self.topic,
                value=enriched_message,
                key=key
            )
            
            # Get result (blocking call with timeout)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"✅ SMS sent successfully:")
            logger.info(f"   📱 Phone: {sms_message.get('phone', 'N/A')}")
            logger.info(f"   📍 Partition: {record_metadata.partition}")
            logger.info(f"   📊 Offset: {record_metadata.offset}")
            
            return True
            
        except KafkaError as e:
            logger.error(f"❌ Kafka error sending SMS: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error sending SMS: {str(e)}")
            return False
    
    def send_bulk_sms(self, sms_list: List[Dict], delay_ms: int = 100) -> Dict:
        """
        Send multiple SMS messages to Kafka
        
        Args:
            sms_list (List[Dict]): List of SMS messages
            delay_ms (int): Delay between messages in milliseconds
            
        Returns:
            Dict: Statistics about sent messages
        """
        stats = {
            'total': len(sms_list),
            'successful': 0,
            'failed': 0,
            'start_time': datetime.now(),
            'errors': []
        }
        
        logger.info(f"🚀 Starting bulk SMS sending: {stats['total']} messages")
        
        for i, sms in enumerate(sms_list, 1):
            try:
                if self.send_sms(sms):
                    stats['successful'] += 1
                else:
                    stats['failed'] += 1
                    stats['errors'].append(f"Message {i}: Failed to send SMS")
                
                # Progress logging
                if i % 10 == 0:
                    logger.info(f"📈 Progress: {i}/{stats['total']} messages sent")
                
                # Add delay to avoid overwhelming the system
                if delay_ms > 0:
                    time.sleep(delay_ms / 1000.0)
                    
            except Exception as e:
                stats['failed'] += 1
                stats['errors'].append(f"Message {i}: {str(e)}")
                logger.error(f"❌ Error sending message {i}: {str(e)}")
        
        stats['end_time'] = datetime.now()
        stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
        
        # Log final statistics
        logger.info(f"📊 Bulk sending completed:")
        logger.info(f"   ✅ Successful: {stats['successful']}")
        logger.info(f"   ❌ Failed: {stats['failed']}")
        logger.info(f"   ⏱️ Duration: {stats['duration']:.2f} seconds")
        
        return stats
    
    def load_sms_from_csv(self, csv_file_path: str) -> List[Dict]:
        """
        Load SMS messages from CSV file
        Expected columns: phone, body, timestamp
        
        Args:
            csv_file_path (str): Path to CSV file
            
        Returns:
            List[Dict]: List of SMS messages
        """
        try:
            sms_list = []
            
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                for row_num, row in enumerate(reader, 1):
                    # Clean and validate row data
                    sms = {
                        'phone': row.get('phone', '').strip(),
                        'body': row.get('body', '').strip(),
                        'timestamp': row.get('timestamp', datetime.now().isoformat()).strip()
                    }
                    
                    if self._validate_sms(sms):
                        sms_list.append(sms)
                    else:
                        logger.warning(f"⚠️ Skipping invalid SMS at row {row_num}")
            
            logger.info(f"📄 Loaded {len(sms_list)} valid SMS messages from {csv_file_path}")
            return sms_list
            
        except FileNotFoundError:
            logger.error(f"❌ CSV file not found: {csv_file_path}")
            return []
        except Exception as e:
            logger.error(f"❌ Error loading CSV file: {str(e)}")
            return []
    
    def simulate_realtime_sms(self, count: int = 10, interval_seconds: int = 2):
        """
        Simulate real-time SMS messages for testing
        
        Args:
            count (int): Number of messages to simulate
            interval_seconds (int): Interval between messages
        """
        logger.info(f"🎭 Starting SMS simulation: {count} messages, {interval_seconds}s intervals")
        
        # Sample SMS templates for simulation
        sms_templates = [
            "Transaction: OMR {amount} at {vendor} on {date}",
            "Debit: OMR {amount} from {vendor} on {date}",
            "You sent OMR {amount} to {vendor} on {date}",
            "Payment of OMR {amount} to {vendor} completed on {date}",
            "Purchase: OMR {amount} at {vendor} - {date}"
        ]
        
        vendors = ["SuperMarket", "Gas Station", "Restaurant", "Online Store", "Pharmacy", 
                  "Coffee Shop", "Bookstore", "Electronics", "Clothing Store", "Hotel"]
        
        phones = ["+96812345678", "+96887654321", "+96811111111", "+96822222222", "+96833333333"]
        
        for i in range(count):
            try:
                # Generate random SMS data
                amount = round(random.uniform(5.0, 500.0), 2)
                vendor = random.choice(vendors)
                phone = random.choice(phones)
                date = (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
                
                body = random.choice(sms_templates).format(
                    amount=amount,
                    vendor=vendor,
                    date=date
                )
                
                sms_message = {
                    "phone": phone,
                    "body": body,
                    "timestamp": datetime.now().isoformat()
                }
                
                success = self.send_sms(sms_message)
                
                if success:
                    logger.info(f"🎭 Simulated SMS {i+1}/{count}: {amount} OMR at {vendor}")
                else:
                    logger.error(f"❌ Failed to send simulated SMS {i+1}")
                
                if i < count - 1:  # Don't sleep after last message
                    time.sleep(interval_seconds)
                    
            except Exception as e:
                logger.error(f"❌ Error in SMS simulation {i+1}: {str(e)}")
        
        logger.info(f"🎭 SMS simulation completed!")
    
    def _validate_sms(self, sms: Dict) -> bool:
        """Validate SMS message format"""
        required_fields = ['phone', 'body']
        
        for field in required_fields:
            if not sms.get(field) or not str(sms[field]).strip():
                logger.warning(f"⚠️ Missing or empty required field: {field}")
                return False
        
        # Basic phone number validation
        phone = str(sms['phone']).strip()
        if len(phone) < 8:
            logger.warning(f"⚠️ Invalid phone number: {phone}")
            return False
        
        return True
    
    def close(self):
        """Close Kafka producer"""
        try:
            self.producer.flush()  # Ensure all messages are sent
            self.producer.close()
            logger.info("🔒 Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"❌ Error closing Kafka producer: {str(e)}")

def main():
    """Main function for testing the SMS producer"""
    producer = None
    
    try:
        # Initialize producer
        producer = SMSKafkaProducer()
        
        # Test 1: Send single SMS
        print("\n" + "="*50)
        print("🧪 TEST 1: Single SMS")
        print("="*50)
        
        test_sms = {
            "phone": "+96812345678",
            "body": "Transaction: OMR 25.50 at SuperMarket on 2024-06-14",
            "timestamp": datetime.now().isoformat()
        }
        
        success = producer.send_sms(test_sms)
        print(f"Single SMS test: {'✅ PASSED' if success else '❌ FAILED'}")
        
        # Test 2: Load from CSV (if exists)
        print("\n" + "="*50)
        print("🧪 TEST 2: Load from CSV")
        print("="*50)
        
        csv_path = "../../data/sms_samples.csv"
        if os.path.exists(csv_path):
            sms_list = producer.load_sms_from_csv(csv_path)
            if sms_list:
                stats = producer.send_bulk_sms(sms_list[:5])  # Send first 5 messages
                print(f"CSV bulk test: {'✅ PASSED' if stats['successful'] > 0 else '❌ FAILED'}")
            else:
                print("❌ No valid SMS messages found in CSV")
        else:
            print(f"⚠️ CSV file not found: {csv_path}")
        
        # Test 3: Real-time simulation
        print("\n" + "="*50)
        print("🧪 TEST 3: Real-time Simulation")
        print("="*50)
        
        producer.simulate_realtime_sms(count=3, interval_seconds=1)
        print("✅ Real-time simulation completed")
        
    except KeyboardInterrupt:
        logger.info("🛑 Producer stopped by user")
    except Exception as e:
        logger.error(f"❌ Producer error: {str(e)}")
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    main()