import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from airflow.hooks.base import BaseHook
from pymongo import MongoClient

try:
    from airflow.sdk import dag, get_current_context, task
except ImportError:
    from airflow.decorators import dag, task
    from airflow.operators.python import get_current_context


API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = "USD,GBP,JPY"
MONGO_CONNECTION_ID = "currency_mongodb"
log = logging.getLogger(__name__)


@dag(
    dag_id="currency_exchange_etl",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["etl", "currency"],
)
def currency_exchange_etl():
    @task(task_id="extract_exchange_rates")
    def extract() -> None:
        response = requests.get(
            API_URL,
            params={"base": BASE_CURRENCY, "quotes": TARGET_CURRENCIES},
            timeout=30,
        )
        response.raise_for_status()
        raw_rates: Any = response.json()
        if not isinstance(raw_rates, list):
            raise ValueError("Frankfurter API response must be a list of rates")
        log.info("Extracted %d rows", len(raw_rates))
        get_current_context()["ti"].xcom_push(key="raw_rates", value=raw_rates)

    @task(task_id="transform_exchange_rates")
    def transform() -> None:
        task_instance = get_current_context()["ti"]
        raw_rates = task_instance.xcom_pull(
            task_ids="extract_exchange_rates",
            key="raw_rates",
        )
        if not isinstance(raw_rates, list):
            raise ValueError("The raw_rates XCom is missing or is not a list")

        retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        documents = [
            {
                "base_currency": row["base"],
                "target_currency": row["quote"],
                "exchange_rate": row["rate"],
                "rate_date": row["date"],
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
            for row in raw_rates
        ]
        log.info("Transformed %d documents", len(documents))
        task_instance.xcom_push(key="transformed_rates", value=documents)

    @task(task_id="load_to_mongodb")
    def load() -> None:
        documents = get_current_context()["ti"].xcom_pull(
            task_ids="transform_exchange_rates",
            key="transformed_rates",
        )
        if not isinstance(documents, list):
            raise ValueError("The transformed_rates XCom is missing or is not a list")

        connection = BaseHook.get_connection(MONGO_CONNECTION_ID)
        with MongoClient(
            host=connection.host,
            port=connection.port or 27017,
            username=connection.login,
            password=connection.password,
            authSource=connection.extra_dejson.get("authSource", "admin"),
            serverSelectionTimeoutMS=10_000,
        ) as client:
            collection = client["currency_db"]["exchange_rates"]
            collection.create_index(
                [
                    ("base_currency", 1),
                    ("target_currency", 1),
                    ("rate_date", 1),
                ],
                unique=True,
            )
            for document in documents:
                collection.update_one(
                    {
                        "base_currency": document["base_currency"],
                        "target_currency": document["target_currency"],
                        "rate_date": document["rate_date"],
                    },
                    {"$set": document},
                    upsert=True,
                )
        log.info("Loaded %d documents", len(documents))

    extract() >> transform() >> load()


currency_exchange_etl()
