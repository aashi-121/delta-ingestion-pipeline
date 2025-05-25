# Delta Ingestion Pipeline

A scheduled data ingestion pipeline using PySpark and Delta Lake with automatic email notifications.

## Features

- Ingests synthetic data using Faker
- Stores data in Delta Lake format
- Sends email notifications with latest snapshot
- Runs periodically using APScheduler

## How to Run

1. Set up a virtual environment
2. Install dependencies:
pip install -r requirements.txt

3. Start the pipeline:
python src/delta_pipeline.py

## Requirements

- Python 3.8+
- PySpark
- delta-spark
- pandas
- faker
- apscheduler
- pytz
- yagmail
