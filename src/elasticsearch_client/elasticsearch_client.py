#!/usr/bin/env python3
"""
Elasticsearch Client for SMS Profile Storage
Handles indexing, searching, and aggregating SMS transaction data
"""

import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, RequestError, NotFoundError
import sys
import os

# Add parent directories to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from config.elasticsearch_config import ELASTICSEARCH_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ElasticsearchClient:
    def __init__(self):
        """Initialize Elasticsearch client with configuration"""
        try:
            self.es = Elasticsearch(
                hosts=ELASTICSEARCH_CONFIG['hosts'],
                # Connection settings
                timeout=30,
                max_retries=3,
                retry_on_timeout=True,
                # Health check
                sniff_on_start=False,
                sniff_on_connection_fail=False,
                verify_certs=False,
                ssl_show_warn=False
            )
            
            self.index_name = ELASTICSEARCH_CONFIG['index_name']
            
            # Test connection
            if self.es.ping():
                logger.info(f"✅ Connected to Elasticsearch at {ELASTICSEARCH_CONFIG['hosts']}")
                self._setup_index()
            else:
                raise ConnectionError("Failed to connect to Elasticsearch")
                
        except Exception as e:
            logger.error(f"❌ Failed to initialize Elasticsearch client: {str(e)}")
            raise
    
    def _setup_index(self):
        """Create index with proper mapping if it doesn't exist"""
        try:
            if not self.es.indices.exists(index=self.index_name):
                # Define mapping for SMS profile data
                mapping = {
                    "mappings": {
                        "properties": {
                            "phone": {
                                "type": "keyword"  # Exact match for phone numbers
                            },
                            "amount": {
                                "type": "float"
                            },
                            "vendor": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword"  # For aggregations
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
                            "timestamp": {
                                "type": "date",
                                "format": "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                            },
                            "original_message": {
                                "type": "text"
                            },
                            "kafka_metadata": {
                                "type": "object",
                                "properties": {
                                    "topic": {"type": "keyword"},
                                    "partition": {"type": "integer"},
                                    "offset": {"type": "long"},
                                    "kafka_timestamp": {"type": "long"},
                                    "consumer_processed_at": {"type": "date"}
                                }
                            },
                            # Fields for ML and profiling
                            "is_fraud": {
                                "type": "boolean"
                            },
                            "fraud_score": {
                                "type": "float"
                            },
                            "user_profile": {
                                "type": "object",
                                "properties": {
                                    "avg_transaction_amount": {"type": "float"},
                                    "transaction_frequency": {"type": "integer"},
                                    "favorite_vendors": {"type": "keyword"},
                                    "spending_pattern": {"type": "keyword"},
                                    "risk_level": {"type": "keyword"}
                                }
                            }
                        }
                    }
                }
                
                # Create index
                self.es.indices.create(index=self.index_name, body=mapping)
                logger.info(f"✅ Created index '{self.index_name}' with mapping")
            else:
                logger.info(f"📊 Index '{self.index_name}' already exists")
                
        except Exception as e:
            logger.error(f"❌ Error setting up index: {str(e)}")
            raise
    
    def index_sms_transaction(self, transaction_data: Dict) -> bool:
        """
        Index a single SMS transaction
        
        Args:
            transaction_data (Dict): Parsed SMS transaction data
            
        Returns:
            bool: True if indexed successfully, False otherwise
        """
        try:
            # Validate required fields
            required_fields = ['phone', 'amount', 'vendor']
            for field in required_fields:
                if field not in transaction_data:
                    logger.error(f"❌ Missing required field: {field}")
                    return False
            
            # Add indexing timestamp
            doc = {
                **transaction_data,
                'indexed_at': datetime.now().isoformat()
            }
            
            # Generate document ID based on phone and timestamp for deduplication
            doc_id = f"{transaction_data['phone']}_{transaction_data.get('timestamp', datetime.now().isoformat())}"
            
            # Index document
            response = self.es.index(
                index=self.index_name,
                id=doc_id,
                body=doc
            )
            
            logger.info(f"✅ Indexed SMS transaction:")
            logger.info(f"   📱 Phone: {transaction_data['phone']}")
            logger.info(f"   💰 Amount: {transaction_data['amount']} OMR")
            logger.info(f"   🏪 Vendor: {transaction_data['vendor']}")
            logger.info(f"   📊 Document ID: {response['_id']}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error indexing SMS transaction: {str(e)}")
            return False
    
    def bulk_index_transactions(self, transactions: List[Dict]) -> Dict:
        """
        Bulk index multiple SMS transactions
        
        Args:
            transactions (List[Dict]): List of parsed SMS transactions
            
        Returns:
            Dict: Statistics about bulk indexing
        """
        try:
            if not transactions:
                logger.warning("📭 No transactions to index")
                return {'successful': 0, 'failed': 0}
            
            # Prepare bulk request
            bulk_body = []
            
            for transaction in transactions:
                # Generate document ID
                doc_id = f"{transaction['phone']}_{transaction.get('timestamp', datetime.now().isoformat())}"
                
                # Add metadata
                doc = {
                    **transaction,
                    'indexed_at': datetime.now().isoformat()
                }
                
                # Index action
                bulk_body.append({
                    "index": {
                        "_index": self.index_name,
                        "_id": doc_id
                    }
                })
                bulk_body.append(doc)
            
            # Execute bulk request
            response = self.es.bulk(body=bulk_body)
            
            # Process response
            stats = {
                'successful': 0,
                'failed': 0,
                'errors': []
            }
            
            for item in response['items']:
                if 'index' in item:
                    if item['index']['status'] in [200, 201]:
                        stats['successful'] += 1
                    else:
                        stats['failed'] += 1
                        stats['errors'].append(item['index'].get('error', 'Unknown error'))
            
            logger.info(f"📦 Bulk indexing completed:")
            logger.info(f"   ✅ Successful: {stats['successful']}")
            logger.info(f"   ❌ Failed: {stats['failed']}")
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error in bulk indexing: {str(e)}")
            return {'successful': 0, 'failed': len(transactions), 'errors': [str(e)]}
    
    def search_user_transactions(self, phone: str, limit: int = 100) -> List[Dict]:
        """
        Search transactions for specific user
        
        Args:
            phone (str): User phone number
            limit (int): Maximum number of results
            
        Returns:
            List[Dict]: List of user transactions
        """
        try:
            query = {
                "query": {
                    "term": {
                        "phone": phone
                    }
                },
                "sort": [
                    {"timestamp": {"order": "desc"}}
                ],
                "size": limit
            }
            
            response = self.es.search(index=self.index_name, body=query)
            
            transactions = []
            for hit in response['hits']['hits']:
                transactions.append({
                    **hit['_source'],
                    '_id': hit['_id'],
                    '_score': hit['_score']
                })
            
            logger.info(f"🔍 Found {len(transactions)} transactions for phone {phone}")
            return transactions
            
        except Exception as e:
            logger.error(f"❌ Error searching user transactions: {str(e)}")
            return []
    
    def get_user_spending_stats(self, phone: str) -> Dict:
        """
        Get spending statistics for a user
        
        Args:
            phone (str): User phone number
            
        Returns:
            Dict: User spending statistics
        """
        try:
            query = {
                "query": {
                    "term": {
                        "phone": phone
                    }
                },
                "aggs": {
                    "total_spent": {
                        "sum": {
                            "field": "amount"
                        }
                    },
                    "avg_amount": {
                        "avg": {
                            "field": "amount"
                        }
                    },
                    "transaction_count": {
                        "value_count": {
                            "field": "amount"
                        }
                    },
                    "top_vendors": {
                        "terms": {
                            "field": "vendor.keyword",
                            "size": 5
                        }
                    },
                    "transaction_types": {
                        "terms": {
                            "field": "transaction_type"
                        }
                    }
                },
                "size": 0  # Only return aggregations
            }
            
            response = self.es.search(index=self.index_name, body=query)
            
            aggs = response['aggregations']
            stats = {
                'phone': phone,
                'total_spent': aggs['total_spent']['value'] or 0,
                'avg_amount': aggs['avg_amount']['value'] or 0,
                'transaction_count': aggs['transaction_count']['value'] or 0,
                'top_vendors': [bucket['key'] for bucket in aggs['top_vendors']['buckets']],
                'transaction_types': {bucket['key']: bucket['doc_count'] for bucket in aggs['transaction_types']['buckets']}
            }
            
            logger.info(f"📊 Generated spending stats for {phone}")
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error getting user spending stats: {str(e)}")
            return {}
    
    def query_users_by_criteria(self, criteria: Dict, limit: int = 100) -> List[Dict]:
        """
        Query users based on specific criteria
        
        Args:
            criteria (Dict): Search criteria (amount_range, vendors, transaction_types, etc.)
            limit (int): Maximum number of results
            
        Returns:
            List[Dict]: List of matching users
        """
        try:
            # Build query based on criteria
            must_clauses = []
            
            # Amount range filter
            if 'amount_range' in criteria:
                must_clauses.append({
                    "range": {
                        "amount": {
                            "gte": criteria['amount_range'].get('min', 0),
                            "lte": criteria['amount_range'].get('max', 10000)
                        }
                    }
                })
            
            # Vendor filter
            if 'vendors' in criteria:
                must_clauses.append({
                    "terms": {
                        "vendor.keyword": criteria['vendors']
                    }
                })
            
            # Transaction type filter
            if 'transaction_types' in criteria:
                must_clauses.append({
                    "terms": {
                        "transaction_type": criteria['transaction_types']
                    }
                })
            
            # Date range filter
            if 'date_range' in criteria:
                must_clauses.append({
                    "range": {
                        "date": {
                            "gte": criteria['date_range'].get('start'),
                            "lte": criteria['date_range'].get('end')
                        }
                    }
                })
            
            query = {
                "query": {
                    "bool": {
                        "must": must_clauses
                    }
                },
                "aggs": {
                    "users": {
                        "terms": {
                            "field": "phone",
                            "size": limit
                        }
                    }
                },
                "size": 0
            }
            
            response = self.es.search(index=self.index_name, body=query)
            
            users = []
            for bucket in response['aggregations']['users']['buckets']:
                users.append({
                    'phone': bucket['key'],
                    'matching_transactions': bucket['doc_count']
                })
            
            logger.info(f"🔍 Found {len(users)} users matching criteria")
            return users
            
        except Exception as e:
            logger.error(f"❌ Error querying users by criteria: {str(e)}")
            return []
    
    def update_fraud_status(self, doc_id: str, is_fraud: bool, fraud_score: float = None) -> bool:
        """
        Update fraud detection results for a transaction
        
        Args:
            doc_id (str): Document ID
            is_fraud (bool): Whether transaction is fraudulent
            fraud_score (float): Fraud probability score
            
        Returns:
            bool: True if updated successfully
        """
        try:
            update_body = {
                "doc": {
                    "is_fraud": is_fraud,
                    "fraud_updated_at": datetime.now().isoformat()
                }
            }
            
            if fraud_score is not None:
                update_body["doc"]["fraud_score"] = fraud_score
            
            response = self.es.update(
                index=self.index_name,
                id=doc_id,
                body=update_body
            )
            
            logger.info(f"✅ Updated fraud status for document {doc_id}: {'FRAUD' if is_fraud else 'CLEAN'}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error updating fraud status: {str(e)}")
            return False
    
    def get_health_status(self) -> Dict:
        """Get Elasticsearch cluster health and index statistics"""
        try:
            # Cluster health
            health = self.es.cluster.health()
            
            # Index stats
            stats = self.es.indices.stats(index=self.index_name)
            
            index_stats = stats['indices'].get(self.index_name, {})
            doc_count = index_stats.get('total', {}).get('docs', {}).get('count', 0)
            index_size = index_stats.get('total', {}).get('store', {}).get('size_in_bytes', 0)
            
            return {
                'cluster_status': health['status'],
                'cluster_name': health['cluster_name'],
                'number_of_nodes': health['number_of_nodes'],
                'index_name': self.index_name,
                'document_count': doc_count,
                'index_size_bytes': index_size,
                'index_size_mb': round(index_size / (1024 * 1024), 2)
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting health status: {str(e)}")
            return {}

# Create global instance
es_client = ElasticsearchClient()

def main():
    """Test the Elasticsearch client"""
    try:
        # Test connection and health
        health = es_client.get_health_status()
        print(f"📊 Elasticsearch Health: {json.dumps(health, indent=2)}")
        
        # Test indexing
        test_transaction = {
            "phone": "+96812345678",
            "amount": 25.50,
            "vendor": "SuperMarket",
            "date": "2024-06-14",
            "transaction_type": "purchase",
            "source": "bank_sms",
            "timestamp": datetime.now().isoformat()
        }
        
        success = es_client.index_sms_transaction(test_transaction)
        print(f"Test indexing: {'✅ PASSED' if success else '❌ FAILED'}")
        
        # Test search
        transactions = es_client.search_user_transactions("+96812345678", limit=10)
        print(f"Test search: {'✅ PASSED' if transactions else '❌ FAILED'}")
        
        # Test stats
        stats = es_client.get_user_spending_stats("+96812345678")
        print(f"Test stats: {'✅ PASSED' if stats else '❌ FAILED'}")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")

if __name__ == "__main__":
    main()