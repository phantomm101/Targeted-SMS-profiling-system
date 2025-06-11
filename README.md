# 📡 Targeted SMS Profiling and Fraud Detection System

This project aims to profile users based on incoming SMS messages and detect fraudulent activity in real-time using machine learning. It leverages **Apache Kafka**, **Elasticsearch**, and **Kibana** to build a powerful data pipeline, along with Python-based services for data ingestion, processing, and analysis.

---

## 🔧 Project Structure

Targeted-SMS-profiling-system/
├── data/ # Store raw SMS datasets
├── logs/ # Logging directory
├── models/ # Trained ML models
├── tests/ # Unit & integration tests
├── config/ # Configuration scripts
├── src/
│ ├── kafka_producer/ # Simulates SMS input
│ ├── kafka_consumer/ # Consumes SMS data
│ ├── elasticsearch_client/ # Indexes to Elasticsearch
│ ├── ml_models/ # Fraud detection logic
│ ├── profiling/ # User profiling logic
│ └── api/ # REST API (Optional)
├── docker/ # Docker Compose setup
├── requirements.txt # Python dependencies
└── README.md # 


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


