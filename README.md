# 📡 Targeted SMS Profiling and Fraud Detection System

This project aims to profile users based on incoming SMS messages and detect fraudulent activity in real-time using machine learning. It leverages **Apache Kafka**, **Elasticsearch**, and **Kibana** to build a powerful data pipeline, along with Python-based services for data ingestion, processing, and analysis.

---

## 🔧 Project Structure

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
├── venv/
├── .gitignore
├── README.md
└── requirements.txt

---

## 🚀 Technologies Used

- [Kafka](https://kafka.apache.org/)
- [Elasticsearch](https://www.elastic.co/elasticsearch/)
- [Kibana](https://www.elastic.co/kibana/)
- [Python 3.9+](https://www.python.org/)
- [Scikit-learn](https://scikit-learn.org/)
- [Docker & Docker Compose](https://docs.docker.com/compose/)

---

## 🧪 Getting Started (Local Setup)

### 1. Clone the Repo

```bash
git clone https://github.com/phantomm101/Targeted-SMS-profiling-system.git
cd Targeted-SMS-profiling-system


