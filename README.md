# 📡 Targeted SMS Profiling and Fraud Detection System

This project aims to profile users based on incoming SMS messages and detect fraudulent activity in real-time using machine learning. It leverages **Apache Kafka**, **Elasticsearch**, and **Kibana** to build a powerful data pipeline, along with Python-based services for data ingestion, processing, and analysis.

---

## 🔧 Project Structure

Targeted-SMS-profiling-system/
├── config/                  ✅ (already exists)
│   ├── app_config.py
│   ├── elasticsearch_config.py
│   └── kafka_config.py
├── data/                    ✅ (created)
├── docker/                  ✅ (already exists)
│   └── docker-compose.yml
├── logs/                    ✅ (created)
├── models/                  ✅ (created)
├── sms_parser_engine/       ✅ (already exists)
│   └── sms_parser.py
├── src/                     ✅ (created)
│   ├── __init__.py
│   ├── api/                 ✅ (created)
│   │   └── __init__.py
│   ├── elasticsearch_client/ ✅ (created)
│   │   └── __init__.py
│   ├── kafka_consumer/      ✅ (created)
│   │   └── __init__.py
│   ├── kafka_producer/      ✅ (created)
│   │   └── __init__.py
│   ├── ml_models/           ✅ (created)
│   │   └── __init__.py
│   └── profiling/           ✅ (created)
│       └── __init__.py
├── tests/                   ✅ (already exists)
├── venv/                    ✅ (already exists)
├── .gitignore               ✅ (already exists)
├── README.md                ✅ (already exists)
└── requirements.txt         ✅ (already exists)

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


