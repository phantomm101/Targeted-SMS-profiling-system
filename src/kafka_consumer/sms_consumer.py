import json
import logging
from typing import Dict, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import re
from datetime import datetime
import hashlib

# Add this import at the top of sms_consumer.py
from src.elasticsearch_client.elasticsearch_client import es_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SMSConsumer:
    def __init__(self, kafka_config: Dict, topic: str):
        """
        Initialize SMS Consumer
        
        Args:
            kafka_config (Dict): Kafka configuration
            topic (str): Kafka topic to consume from
        """
        self.kafka_config = kafka_config
        self.topic = topic
        self.consumer = None
        
        # SMS parsing patterns
        self.patterns = {
            'bank_transfer': [
                r'transferred\s+OMR\s+([\d,]+\.?\d*)',
                r'sent\s+OMR\s+([\d,]+\.?\d*)',
                r'paid\s+OMR\s+([\d,]+\.?\d*)'
            ],
            'mobile_money': [
                r'received\s+OMR\s+([\d,]+\.?\d*)',
                r'credited\s+OMR\s+([\d,]+\.?\d*)',
                r'deposited\s+OMR\s+([\d,]+\.?\d*)'
            ],
            'merchant': [
                r'at\s+([A-Za-z\s]+)\s+for\s+OMR',
                r'to\s+([A-Za-z\s]+)\s+OMR',
                r'from\s+([A-Za-z\s]+)'
            ],
            'balance': [
                r'balance\s+is\s+OMR\s+([\d,]+\.?\d*)',
                r'available\s+balance\s+OMR\s+([\d,]+\.?\d*)'
            ]
        }
    
    def connect(self):
        """Connect to Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.kafka_config.get('bootstrap_servers', ['localhost:9092']),
                group_id=self.kafka_config.get('group_id', 'sms-consumer-group'),
                auto_offset_reset=self.kafka_config.get('auto_offset_reset', 'latest'),
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info(f"✅ Connected to Kafka topic: {self.topic}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {str(e)}")
            return False
    
    def parse_sms(self, sms_data: Dict) -> Optional[Dict]:
        """
        Parse SMS message and extract structured data
        
        Args:
            sms_data (Dict): Raw SMS data from Kafka
            
        Returns:
            Dict: Parsed SMS data or None if parsing fails
        """
        try:
            message = sms_data.get('message', '').lower()
            phone = sms_data.get('phone', '')
            timestamp = sms_data.get('timestamp', datetime.now().isoformat())
            sender = sms_data.get('sender', '')
            
            parsed_data = {
                'id': self._generate_transaction_id(sms_data),
                'phone': phone,
                'sender': sender,
                'original_message': sms_data.get('message', ''),
                'timestamp': timestamp,
                'parsed_at': datetime.now().isoformat(),
                'transaction_type': None,
                'amount': None,
                'merchant': None,
                'balance': None,
                'currency': 'OMR'
            }
            
            # Extract amount
            amount = self._extract_amount(message)
            if amount:
                parsed_data['amount'] = amount
            
            # Determine transaction type
            transaction_type = self._determine_transaction_type(message)
            parsed_data['transaction_type'] = transaction_type
            
            # Extract merchant/vendor
            merchant = self._extract_merchant(message)
            if merchant:
                parsed_data['merchant'] = merchant
            
            # Extract balance if present
            balance = self._extract_balance(message)
            if balance:
                parsed_data['balance'] = balance
            
            # Add metadata
            parsed_data['metadata'] = {
                'message_length': len(message),
                'contains_balance': balance is not None,
                'contains_merchant': merchant is not None,
                'processing_timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"📱 Parsed SMS from {phone}: {transaction_type} - OMR {amount}")
            return parsed_data
            
        except Exception as e:
            logger.error(f"❌ Error parsing SMS: {str(e)}")
            return None
    
    def _generate_transaction_id(self, sms_data: Dict) -> str:
        """Generate unique transaction ID"""
        content = f"{sms_data.get('phone', '')}{sms_data.get('message', '')}{sms_data.get('timestamp', '')}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def _extract_amount(self, message: str) -> Optional[float]:
        """Extract transaction amount from message"""
        amount_patterns = [
            r'OMR\s+([\d,]+\.?\d*)',
            r'([\d,]+\.?\d*)\s+OMR',
            r'amount\s+([\d,]+\.?\d*)',
            r'([\d,]+\.?\d*)\s+omr'
        ]
        
        for pattern in amount_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                try:
                    amount_str = match.group(1).replace(',', '')
                    return float(amount_str)
                except ValueError:
                    continue
        return None
    
    def _determine_transaction_type(self, message: str) -> str:
        """Determine transaction type from message"""
        if any(word in message for word in ['transferred', 'sent', 'paid', 'withdraw']):
            return 'debit'
        elif any(word in message for word in ['received', 'credited', 'deposited', 'refund']):
            return 'credit'
        elif any(word in message for word in ['balance', 'available']):
            return 'balance_inquiry'
        else:
            return 'unknown'
    
    def _extract_merchant(self, message: str) -> Optional[str]:
        """Extract merchant/vendor name from message"""
        merchant_patterns = [
            r'at\s+([A-Za-z\s]+?)\s+for',
            r'to\s+([A-Za-z\s]+?)\s+OMR',
            r'from\s+([A-Za-z\s]+?)\s+',
            r'merchant\s+([A-Za-z\s]+)'
        ]
        
        for pattern in merchant_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                merchant = match.group(1).strip()
                if len(merchant) > 2 and merchant.lower() not in ['the', 'and', 'for', 'omr']:
                    return merchant.title()
        return None
    
    def _extract_balance(self, message: str) -> Optional[float]:
        """Extract account balance from message"""
        balance_patterns = [
            r'balance\s+is\s+OMR\s+([\d,]+\.?\d*)',
            r'available\s+balance\s+OMR\s+([\d,]+\.?\d*)',
            r'balance:\s+OMR\s+([\d,]+\.?\d*)'
        ]
        
        for pattern in balance_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                try:
                    balance_str = match.group(1).replace(',', '')
                    return float(balance_str)
                except ValueError:
                    continue
        return None
    
    # Replace the send_to_elasticsearch method with this:
    def send_to_elasticsearch(self, parsed_data: Dict):
        """
        Send parsed SMS data to Elasticsearch
        
        Args:
            parsed_data (Dict): Parsed and enriched SMS data
        """
        try:
            # Index the transaction using the Elasticsearch client
            success = es_client.index_sms_transaction(parsed_data)
            
            if success:
                logger.info(f"✅ Transaction indexed to Elasticsearch:")
                logger.info(f"   📊 Index: {es_client.index_name}")
                logger.info(f"   📱 Phone: {parsed_data.get('phone', 'N/A')}")
                logger.info(f"   💰 Amount: {parsed_data.get('amount', 'N/A')} OMR")
            else:
                logger.error(f"❌ Failed to index transaction to Elasticsearch")
                raise Exception("Elasticsearch indexing failed")
                
        except Exception as e:
            logger.error(f"❌ Error sending to Elasticsearch: {str(e)}")
            raise
    
    def process_message(self, message_data: Dict):
        """
        Process individual SMS message
        
        Args:
            message_data (Dict): SMS message data from Kafka
        """
        try:
            # Parse the SMS message
            parsed_data = self.parse_sms(message_data)
            
            if parsed_data:
                # Send to Elasticsearch
                self.send_to_elasticsearch(parsed_data)
                
                # Log successful processing
                logger.info(f"✅ Successfully processed SMS from {parsed_data.get('phone')}")
            else:
                logger.warning(f"⚠️ Failed to parse SMS message")
                
        except Exception as e:
            logger.error(f"❌ Error processing message: {str(e)}")
    
    def start_consuming(self):
        """Start consuming messages from Kafka"""
        if not self.consumer:
            if not self.connect():
                return False
        
        try:
            logger.info(f"🔄 Starting to consume messages from topic: {self.topic}")
            
            for message in self.consumer:
                try:
                    # Process the message
                    self.process_message(message.value)
                    
                except KeyboardInterrupt:
                    logger.info("🛑 Stopping consumer...")
                    break
                except Exception as e:
                    logger.error(f"❌ Error processing message: {str(e)}")
                    continue
                    
        except KafkaError as e:
            logger.error(f"❌ Kafka error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error: {str(e)}")
            return False
        finally:
            self.close()
    
    def close(self):
        """Close Kafka consumer connection"""
        if self.consumer:
            self.consumer.close()
            logger.info("✅ Kafka consumer connection closed")

# Example usage and testing
if __name__ == "__main__":
    # Kafka configuration
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],
        'group_id': 'sms-consumer-group',
        'auto_offset_reset': 'latest'
    }
    
    # Create and start consumer
    consumer = SMSConsumer(kafka_config, 'sms-transactions')
    
    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped by user")
    except Exception as e:
        logger.error(f"❌ Consumer error: {str(e)}")
    finally:
        consumer.close()