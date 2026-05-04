#!/usr/bin/env python3
"""
Elasticsearch Configuration
Configuration settings for Elasticsearch connection and indexing
"""

# Elasticsearch connection configuration
ELASTICSEARCH_CONFIG = {
    # Elasticsearch hosts - include scheme, host, and port
    'hosts': [
        {
            'host': 'localhost',
            'port': 9200,
            'scheme': 'http'  # Use 'https' for secure connections
        }
    ],
    
    # Alternative simple URL format
    # 'hosts': ['http://localhost:9200'],
    
    # Index configuration
    'index_name': 'sms-profiles',
    
    # Connection settings
    'timeout': 30,
    'max_retries': 3,
    'retry_on_timeout': True,
    
    # SSL settings (for production)
    'verify_certs': False,
    'ssl_show_warn': False,
    
    # Authentication (if needed)
    # 'http_auth': ('username', 'password'),
    # 'api_key': ('api_key_id', 'api_key_secret'),
    
    # Additional settings
    'request_timeout': 30,
    'connection_timeout': 10,
}

# Index mapping template for SMS data
SMS_INDEX_MAPPING = {
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
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "1s"
    }
}

# Kibana dashboard configuration
KIBANA_CONFIG = {
    'host': 'localhost',
    'port': 5601,
    'url': 'http://localhost:5601'
}

def get_elasticsearch_url():
    """Get formatted Elasticsearch URL"""
    host_config = ELASTICSEARCH_CONFIG['hosts'][0]
    return f"{host_config['scheme']}://{host_config['host']}:{host_config['port']}"

def validate_elasticsearch_config():
    """Validate Elasticsearch configuration"""
    try:
        hosts = ELASTICSEARCH_CONFIG.get('hosts', [])
        if not hosts:
            raise ValueError("No Elasticsearch hosts configured")
        
        for host in hosts:
            if isinstance(host, dict):
                required_fields = ['host', 'port', 'scheme']
                missing_fields = [field for field in required_fields if field not in host]
                if missing_fields:
                    raise ValueError(f"Missing required fields in host config: {missing_fields}")
            elif isinstance(host, str):
                if not host.startswith(('http://', 'https://')):
                    raise ValueError(f"Host URL must include scheme: {host}")
        
        return True
        
    except Exception as e:
        print(f"❌ Elasticsearch configuration validation failed: {str(e)}")
        return False

if __name__ == "__main__":
    print("🔧 Elasticsearch Configuration")
    print("=" * 50)
    
    if validate_elasticsearch_config():
        print("✅ Configuration is valid")
        print(f"📡 Elasticsearch URL: {get_elasticsearch_url()}")
        print(f"📊 Index name: {ELASTICSEARCH_CONFIG['index_name']}")
        print(f"🖥️ Kibana URL: {KIBANA_CONFIG['url']}")
    else:
        print("❌ Configuration is invalid")
        
    print("\nConfiguration details:")
    for key, value in ELASTICSEARCH_CONFIG.items():
        print(f"  {key}: {value}")