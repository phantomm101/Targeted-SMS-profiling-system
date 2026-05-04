#!/usr/bin/env python3
"""
Elasticsearch Client for SMS Data
Handles indexing and querying of processed SMS messages
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, RequestError, NotFoundError
import sys
import os

# Add parent directories to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

# Import configuration - create a local config if the import fails
try:
    from config.elasticsearch_config import ELASTICSEARCH_CONFIG
except ImportError:
    # Fallback configuration
    ELASTICSEARCH_CONFIG = {
        'hosts': ['http://localhost:9200'],
        'index_name': 'sms-profiles',
        'request_timeout': 30,
        'timeout': 30,
        'max_retries': 3,
        'retry_on_timeout': True,
        'verify_certs': False,
        'ssl_show_warn': False
    }

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SMSElasticsearchClient:
    def __init__(self):
        """Initialize Elasticsearch client with configuration"""
        try:
            self.client = Elasticsearch(
                hosts=ELASTICSEARCH_CONFIG['hosts'],
                # Disable SSL verification for development
                verify_certs=ELASTICSEARCH_CONFIG.get('verify_certs', False),
                ssl_show_warn=ELASTICSEARCH_CONFIG.get('ssl_show_warn', False),
                # Connection settings
                request_timeout=ELASTICSEARCH_CONFIG.get('request_timeout', 30),
                max_retries=ELASTICSEARCH_CONFIG.get('max_retries', 3),
                retry_on_timeout=ELASTICSEARCH_CONFIG.get('retry_on_timeout', True)
            )
            
            self.index_name = ELASTICSEARCH_CONFIG['index_name']
            
            # Test connection and setup
            self._test_connection()
            self._setup_index()
            
            logger.info(f"✅ Elasticsearch client initialized successfully")
            logger.info(f"📊 Target index: {self.index_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Elasticsearch client: {str(e)}")
            raise
    
    def _test_connection(self):
        """Test Elasticsearch connection"""
        try:
            if self.client.ping():
                logger.info("🔗 Successfully connected to Elasticsearch")
                
                # Get cluster info
                info = self.client.info()
                logger.info(f"📡 Elasticsearch version: {info.get('version', {}).get('number', 'Unknown')}")
            else:
                raise ConnectionError("Failed to ping Elasticsearch")
                
        except Exception as e:
            logger.error(f"❌ Elasticsearch connection test failed: {str(e)}")
            raise
    
    def _setup_index(self):
        """Create index with proper mapping if it doesn't exist"""
        try:
            # Check if index exists
            if self.client.indices.exists(index=self.index_name):
                logger.info(f"📊 Index '{self.index_name}' already exists")
                return
            
            # Define mapping for SMS data
            mapping = {
                "mappings": {
                    "properties": {
                        "phone": {
                            "type": "keyword"
                        },
                        "amount": {
                            "type": "double"
                        },
                        "vendor": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "date": {
                            "type": "date",
                            "format": "yyyy-MM-dd||yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                        },
                        "transaction_type": {
                            "type": "keyword"
                        },
                        "source": {
                            "type": "keyword"
                        },
                        "original_message": {
                            "type": "text"
                        },
                        "timestamp": {
                            "type": "date",
                            "format": "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                        },
                        "kafka_metadata": {
                            "properties": {
                                "topic": {"type": "keyword"},
                                "partition": {"type": "integer"},
                                "offset": {"type": "long"},
                                "kafka_timestamp": {"type": "long"},
                                "consumer_processed_at": {"type": "date"}
                            }
                        },
                        "indexed_at": {
                            "type": "date"
                        }
                    }
                }
            }
            
            # Create index with mapping
            self.client.indices.create(
                index=self.index_name,
                body=mapping
            )
            
            logger.info(f"✅ Created index '{self.index_name}' with SMS mapping")
            
        except RequestError as e:
            if e.error == 'resource_already_exists_exception':
                logger.info(f"📊 Index '{self.index_name}' already exists")
            else:
                logger.error(f"❌ Error creating index: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"❌ Error setting up index: {str(e)}")
            raise
    
    def index_sms(self, sms_data: Dict, doc_id: Optional[str] = None) -> bool:
        """
        Index a single SMS document
        
        Args:
            sms_data (Dict): Parsed SMS data
            doc_id (str, optional): Document ID (auto-generated if None)
            
        Returns:
            bool: True if indexed successfully, False otherwise
        """
        try:
            # Add indexing timestamp
            enriched_data = {
                **sms_data,
                'indexed_at': datetime.now().isoformat()
            }
            
            # Generate document ID if not provided
            if not doc_id:
                phone = sms_data.get('phone', 'unknown')
                timestamp = sms_data.get('timestamp', datetime.now().isoformat())
                doc_id = f"{phone}_{timestamp}_{hash(str(sms_data)) % 10000}"
            
            # Index the document
            response = self.client.index(
                index=self.index_name,
                id=doc_id,
                body=enriched_data,
                refresh='wait_for'  # Wait for refresh to make it searchable
            )
            
            logger.info(f"✅ SMS indexed successfully:")
            logger.info(f"   📄 Document ID: {response['_id']}")
            logger.info(f"   📱 Phone: {sms_data.get('phone', 'N/A')}")
            logger.info(f"   💰 Amount: {sms_data.get('amount', 'N/A')} OMR")
            logger.info(f"   🏪 Vendor: {sms_data.get('vendor', 'N/A')}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error indexing SMS: {str(e)}")
            logger.error(f"🔍 SMS data: {json.dumps(sms_data, indent=2)}")
            return False
    
    def bulk_index_sms(self, sms_list: List[Dict]) -> Dict:
        """
        Bulk index multiple SMS documents
        
        Args:
            sms_list (List[Dict]): List of parsed SMS data
            
        Returns:
            Dict: Statistics about bulk indexing
        """
        if not sms_list:
            logger.warning("⚠️ No SMS data provided for bulk indexing")
            return {'successful': 0, 'failed': 0, 'errors': []}
        
        logger.info(f"📦 Starting bulk SMS indexing: {len(sms_list)} documents")
        
        # Prepare bulk operations
        bulk_operations = []
        for i, sms_data in enumerate(sms_list):
            try:
                # Generate document ID
                phone = sms_data.get('phone', 'unknown')
                timestamp = sms_data.get('timestamp', datetime.now().isoformat())
                doc_id = f"{phone}_{timestamp}_{i}"
                
                # Add indexing timestamp
                enriched_data = {
                    **sms_data,
                    'indexed_at': datetime.now().isoformat()
                }
                
                # Add index operation
                bulk_operations.append({
                    "index": {
                        "_index": self.index_name,
                        "_id": doc_id
                    }
                })
                bulk_operations.append(enriched_data)
                
            except Exception as e:
                logger.error(f"❌ Error preparing bulk operation {i}: {str(e)}")
        
        # Execute bulk operation
        try:
            response = self.client.bulk(
                body=bulk_operations,
                refresh='wait_for'
            )
            
            # Process response
            stats = {
                'successful': 0,
                'failed': 0,
                'errors': []
            }
            
            for item in response['items']:
                if 'index' in item:
                    if item['index'].get('status') in [200, 201]:
                        stats['successful'] += 1
                    else:
                        stats['failed'] += 1
                        stats['errors'].append(item['index'].get('error', 'Unknown error'))
            
            logger.info(f"📊 Bulk indexing completed:")
            logger.info(f"   ✅ Successful: {stats['successful']}")
            logger.info(f"   ❌ Failed: {stats['failed']}")
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error during bulk indexing: {str(e)}")
            return {'successful': 0, 'failed': len(sms_list), 'errors': [str(e)]}
    
    def search_sms(self, query: Dict, size: int = 10) -> Dict:
        """
        Search SMS documents
        
        Args:
            query (Dict): Elasticsearch query
            size (int): Number of results to return
            
        Returns:
            Dict: Search results
        """
        try:
            response = self.client.search(
                index=self.index_name,
                body=query,
                size=size
            )
            
            logger.info(f"🔍 Search completed: {response['hits']['total']['value']} total hits")
            
            return {
                'total': response['hits']['total']['value'],
                'hits': response['hits']['hits']
            }
            
        except Exception as e:
            logger.error(f"❌ Error searching SMS: {str(e)}")
            return {'total': 0, 'hits': []}
    
    def get_user_profile(self, phone: str, days: int = 30) -> Dict:
        """
        Get user spending profile
        
        Args:
            phone (str): Phone number
            days (int): Number of days to look back
            
        Returns:
            Dict: User profile data
        """
        try:
            # Define aggregation query
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"phone": phone}},
                            {"range": {"date": {"gte": f"now-{days}d"}}}
                        ]
                    }
                },
                "aggs": {
                    "total_spent": {"sum": {"field": "amount"}},
                    "avg_transaction": {"avg": {"field": "amount"}},
                    "transaction_count": {"value_count": {"field": "amount"}},
                    "top_vendors": {
                        "terms": {"field": "vendor.keyword", "size": 5}
                    },
                    "transaction_types": {
                        "terms": {"field": "transaction_type", "size": 10}
                    },
                    "spending_by_day": {
                        "date_histogram": {
                            "field": "date",
                            "calendar_interval": "1d"
                        },
                        "aggs": {
                            "daily_total": {"sum": {"field": "amount"}}
                        }
                    }
                }
            }
            
            response = self.client.search(
                index=self.index_name,
                body=query,
                size=0  # We only want aggregations
            )
            
            # Process aggregations
            aggs = response.get('aggregations', {})
            
            profile = {
                'phone': phone,
                'period_days': days,
                'total_spent': aggs.get('total_spent', {}).get('value', 0),
                'avg_transaction': aggs.get('avg_transaction', {}).get('value', 0),
                'transaction_count': aggs.get('transaction_count', {}).get('value', 0),
                'top_vendors': [
                    {
                        'vendor': bucket['key'],
                        'count': bucket['doc_count']
                    }
                    for bucket in aggs.get('top_vendors', {}).get('buckets', [])
                ],
                'transaction_types': [
                    {
                        'type': bucket['key'],
                        'count': bucket['doc_count']
                    }
                    for bucket in aggs.get('transaction_types', {}).get('buckets', [])
                ],
                'daily_spending': [
                    {
                        'date': bucket['key_as_string'],
                        'amount': bucket['daily_total']['value']
                    }
                    for bucket in aggs.get('spending_by_day', {}).get('buckets', [])
                ]
            }
            
            logger.info(f"👤 Generated profile for {phone}: {profile['transaction_count']} transactions, {profile['total_spent']:.2f} OMR total")
            
            return profile
            
        except Exception as e:
            logger.error(f"❌ Error generating user profile: {str(e)}")
            return {}
    
    def close(self):
        """Close Elasticsearch client"""
        try:
            # Elasticsearch client doesn't need explicit closing
            logger.info("🔒 Elasticsearch client closed successfully")
        except Exception as e:
            logger.error(f"❌ Error closing Elasticsearch client: {str(e)}")

def main():
    """Main function for testing the Elasticsearch client"""
    client = None
    
    try:
        print("\n" + "="*60)
        print("🧪 SMS Elasticsearch Client Test")
        print("="*60)
        
        # Initialize client
        client = SMSElasticsearchClient()
        
        # Test 1: Index single SMS
        print("\n🧪 TEST 1: Index Single SMS")
        test_sms = {
            "phone": "+96812345678",
            "amount": 125.50,
            "vendor": "Test Store",
            "date": "2024-06-16",
            "transaction_type": "purchase",
            "source": "test",
            "original_message": "Test SMS message",
            "timestamp": datetime.now().isoformat()
        }
        
        success = client.index_sms(test_sms)
        print(f"Single SMS indexing: {'✅ PASSED' if success else '❌ FAILED'}")
        
        # Test 2: Search SMS
        print("\n🧪 TEST 2: Search SMS")
        search_query = {
            "query": {
                "match": {
                    "phone": "+96812345678"
                }
            }
        }
        
        results = client.search_sms(search_query)
        print(f"Search test: {'✅ PASSED' if results['total'] > 0 else '❌ FAILED'}")
        print(f"Found {results['total']} matching documents")
        
        # Test 3: User profile
        print("\n🧪 TEST 3: User Profile")
        profile = client.get_user_profile("+96812345678", days=30)
        print(f"Profile test: {'✅ PASSED' if profile else '❌ FAILED'}")
        if profile:
            print(f"Profile: {profile['transaction_count']} transactions, {profile['total_spent']:.2f} OMR")
        
        print("\n✅ All tests completed!")
        
    except Exception as e:
        logger.error(f"❌ Test error: {str(e)}")
    
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    main()