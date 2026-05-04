# 📡 Targeted SMS Profiling and Fraud Detection System

This project aims to profile users based on incoming SMS messages and detect fraudulent activity in real-time using machine learning. It leverages **Apache Kafka**, **Elasticsearch**, and **Kibana** to build a powerful data pipeline, along with Python-based services for data ingestion, processing, and analysis.

---

## 🔧 Project Structure

```text
Targeted-SMS-profiling-system/
├── config/
│   ├── app_config.py
│   ├── elasticsearch_config.py
│   └── kafka_config.py
├── data/
├── docker/
│   └── docker-compose.yml
├── logs/
├── models/
├── sms_parser_engine/
│   └── sms_parser.py
├── src/
│   ├── __init__.py
│   ├── api/
│   │   └── __init__.py
│   ├── elasticsearch_client/
│   │   └── __init__.py
│   ├── kafka_consumer/
│   │   └── __init__.py
│   ├── kafka_producer/
│   │   └── __init__.py
│   ├── ml_models/
│   │   └── __init__.py
│   └── profiling/
│       └── __init__.py
├── tests/
│   └── test_elasticsearch.py
├── .gitignore
├── README.md
└── requirements.txt
```

---

## 🚀 Technologies Used

- [Kafka](https://kafka.apache.org/)
- [Elasticsearch](https://www.elastic.co/elasticsearch/)
- [Kibana](https://www.elastic.co/kibana/)
- [Python 3.9+](https://www.python.org/)
- [Scikit-learn](https://scikit-learn.org/)
- [Docker & Docker Compose](https://docs.docker.com/compose/)

---

## ✅ Prerequisites

Before you begin, make sure you have the following installed on your machine:

| Tool | Version | Installation |
|------|---------|-------------|
| Python | 3.9+ | https://www.python.org/downloads/ |
| Docker | Latest | https://docs.docker.com/get-docker/ |
| Docker Compose | Latest | Included with Docker Desktop |
| Git | Latest | https://git-scm.com/ |

---

## 🧪 Getting Started (Local Setup)

### 1. Clone the Repo

```bash
git clone https://github.com/phantomm101/Targeted-SMS-profiling-system.git
cd Targeted-SMS-profiling-system
```

### 2. Create and Activate a Virtual Environment

**Linux / macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

> **Note:** The project also uses `spaCy`. After installing the requirements, download the English language model:
> ```bash
> python -m spacy download en_core_web_sm
> ```

### 4. Start Infrastructure Services (Docker)

All required services (Kafka, Zookeeper, Elasticsearch, Kibana) are defined in `docker/docker-compose.yml`.

```bash
docker compose -f docker/docker-compose.yml up -d
```

Wait about 30–60 seconds for all services to fully start, then verify they are running:

```bash
docker compose -f docker/docker-compose.yml ps
```

You should see all four services (`zookeeper`, `kafka`, `elasticsearch`, `kibana`) with a status of **Up**.

#### Service URLs

| Service | URL |
|---------|-----|
| Kafka broker | `localhost:9092` |
| Elasticsearch | http://localhost:9201 |
| Kibana | http://localhost:5601 |

### 5. Verify Elasticsearch is Running

```bash
curl http://localhost:9201
```

A successful response returns a JSON object with Elasticsearch cluster information.

### 6. (Optional) Verify the Configuration

You can validate the Elasticsearch configuration by running:

```bash
python config/elasticsearch_config.py
```

Expected output:
```
✅ Configuration is valid
📡 Elasticsearch URL: http://localhost:9200
📊 Index name: sms-profiles
🖥️ Kibana URL: http://localhost:5601
```

---

## 🧑‍💻 Running the SMS Parser

The SMS parser engine extracts transaction data from raw SMS message strings. You can run it directly:

```bash
python sms_parser_engine/sms_parser.py
```

To use it in your own script:

```python
from sms_parser_engine.sms_parser import SMSParser

parser = SMSParser()

sms = {
    "body": "OMR 25.50 at SuperMarket on 2024-06-10",
    "phone": "+96812345678",
    "timestamp": "2024-06-10T10:30:00"
}

result = parser.parse_sms(sms)
print(result)
```

Supported SMS formats:

| Format | Example |
|--------|---------|
| OMR transaction | `OMR 25.50 at SuperMarket on 2024-06-10` |
| Bank debit | `Debit: OMR 100.00 from ATM on 10/06/2024` |
| Mobile money transfer | `You sent OMR 50.00 to John on 2024-06-10` |
| Generic OMR amount | `Your balance is 200.00 OMR` |

---

## 🧪 Running the Tests

### Elasticsearch Connection Test

Make sure Docker services are running (Step 4), then run:

```bash
python tests/test_elasticsearch.py
```

Expected output when Elasticsearch is reachable:
```
Elasticsearch is connected!
Test document indexed!
```

If the connection fails, you will see:
```
Elasticsearch connection failed!
```
Check that the Docker containers are running and that port `9201` is not blocked.

---

## 🛑 Stopping the Services

To stop all Docker containers when you are done:

```bash
docker compose -f docker/docker-compose.yml down
```

To stop the containers **and remove all stored data** (volumes):

```bash
docker compose -f docker/docker-compose.yml down -v
```

---

## 🐛 Troubleshooting

| Problem | Solution |
|---------|---------|
| `Elasticsearch connection failed` | Ensure Docker containers are running: `docker compose -f docker/docker-compose.yml ps` |
| `Port 9201 already in use` | Stop any existing Elasticsearch instance or change the port mapping in `docker/docker-compose.yml` |
| `Port 9092 already in use` | Stop any local Kafka instance or change the port mapping in `docker/docker-compose.yml` |
| `ModuleNotFoundError` | Activate the virtual environment and re-run `pip install -r requirements.txt` |
| `spaCy model not found` | Run `python -m spacy download en_core_web_sm` |
| Kibana not loading | Wait a minute for it to initialise, then refresh http://localhost:5601 |

---

## 📬 Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

