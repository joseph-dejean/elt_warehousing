# ETL Pipeline with Kafka and Snowflake

A complete ETL pipeline that demonstrates real-time data streaming using Apache Kafka (Redpanda) and Snowflake automation.

## Architecture

```
[Data Generator] → [Kafka Producer] → [Redpanda] → [Kafka Consumer] → [Snowflake RAW]
                                                                    ↓
[Snowflake Stream] → [Snowflake Task] → [Snowflake DWH] → [Monitoring Dashboard]
```

## Project Structure

```
etl_project/
├── src/
│   ├── data_generator.py      # Generate fake e-commerce data
│   ├── event_producer.py      # Kafka producer for order events
│   ├── event_consumer.py      # Kafka consumer to Snowflake
│   └── partitioner.py         # Custom Kafka partitioner
├── snowflake/
│   ├── 01_ddl.sql            # Database and table creation
│   ├── 02_merge.sql          # MERGE logic for DWH updates
│   ├── 03_stream_task.sql    # Stream and Task automation
│   └── 04_validation.sql     # Data validation queries
├── monitoring/
│   └── app.py                # Streamlit monitoring dashboard
├── docker-compose.yml        # Redpanda and services setup
├── Dockerfile               # Python environment
├── setup_database.py        # Database setup script
├── setup_automation.py      # Stream and Task setup
├── demo_events.py           # Demo script for videos
└── env.example              # Environment variables template
```

##  Quick Start

### 1. Prerequisites
- Python 3.11+
- Docker Desktop
- Snowflake account

### 2. Setup Environment
```bash
# Clone the repository
git clone <your-repo-url>
cd etl_project

# Install dependencies
pip install kafka-python python-dotenv pandas faker snowflake-connector-python streamlit

# Configure environment
cp env.example .env
# Edit .env with your Snowflake credentials
```

### 3. Start Infrastructure
```bash
# Start Redpanda (Kafka)
docker compose up -d redpanda

# Setup Snowflake database and tables
python setup_database.py

# Setup automation (Stream + Task)
python setup_automation.py
```

### 4. Run the Pipeline
```bash
# Terminal 1: Start consumer
python src/event_consumer.py

# Terminal 2: Start producer
python src/event_producer.py

# Terminal 3: Start monitoring (optional)
streamlit run monitoring/app.py
```

## Data Flow

### 1. Data Generation
- **Source**: `src/data_generator.py`
- **Output**: 100 orders, customers, and products in `RETAIL.RAW.ORDER`

### 2. Event Streaming
- **Producer**: `src/event_producer.py` generates order status events
- **Kafka**: Events stream through Redpanda topic "orders"
- **Consumer**: `src/event_consumer.py` processes events and writes to Snowflake

### 3. Snowflake Automation
- **Stream**: `RETAIL.RAW.EVENTS_STRM` tracks changes to EVENTS table
- **Task**: `TASK_AUTO_UPDATE_ORDER_STATUS` runs every 2 minutes
- **DWH**: `RETAIL.DWH.ORDER_STATUS` maintains current order status

### 4. Monitoring
- **Dashboard**: Streamlit app at `http://localhost:8501`
- **Metrics**: Real-time order status distribution and event counts


##  Technologies Used

- **Python**: Data generation, Kafka clients, Snowflake connector
- **Apache Kafka (Redpanda)**: Real-time event streaming
- **Snowflake**: Data warehouse, Streams, Tasks, automation
- **Streamlit**: Monitoring dashboard
- **Docker**: Containerized infrastructure

##  Key Features

- **Real-time streaming**: Kafka producer/consumer pattern
- **Automated processing**: Snowflake Streams and Tasks
- **Data validation**: Comprehensive validation queries
- **Monitoring**: Real-time dashboard with metrics
- **Scalable architecture**: Docker-based infrastructure

##  Configuration

### Environment Variables
See `env.example` for required configuration:
- Kafka/Redpanda settings
- Snowflake connection parameters

### Snowflake Setup
- Database: `RETAIL`
- Schemas: `RAW` (events), `DWH` (processed data)
- Tables: `ORDER`, `EVENTS`, `ORDER_STATUS`
- Automation: Stream + Task for real-time updates





