from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9201}])

# Test connection
if es.ping():
    print("Elasticsearch is connected!")
    
    # Create test index
    test_doc = {
        'phone': '+1234567890',
        'amount': 25.50,
        'vendor': 'SuperMarket',
        'timestamp': '2024-06-10T10:30:00'
    }
    
    es.index(index='sms-profiles', body=test_doc)
    print("Test document indexed!")
else:
    print("Elasticsearch connection failed!")