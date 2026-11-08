# Building Airflow ETL Pipeline

**Name:** Lumia Noman Qureshi<br>
**Roll number:** 23L-0785<br>
**Section:** BSCS-7A<br>
**Date:** November 8, 2026<br>
**Repository:** [MLOPS_TASK_03](https://github.com/lumia-Qcode/MLOPS_TASK_03)

## Introduction

This project implements a daily ETL pipeline with Astro CLI and Apache Airflow. It
extracts EUR exchange rates for USD, GBP, and JPY from the Frankfurter API, transforms
each rate into the required document structure, and loads the results into MongoDB.
Mongo Express provides a browser interface for inspecting the stored documents.

## Environment and project setup

Docker Desktop, WSL 2, Git, and Astro CLI are used to run the local Airflow project.
The project was initialized with `astro dev init` and contains the DAG, dependencies,
and Docker configuration.

![Docker Engine version and platform command output](screenshots/01-docker-engine.png)

![Astro CLI version command output](screenshots/02-astro-version.png)

![Astro project structure command output](screenshots/03-project-structure.png)

## Dependencies

The DAG uses `requests` to call the Frankfurter API and `pymongo` to write the
transformed records to MongoDB.

![requirements.txt](screenshots/04-requirements.png)

## Docker services, networking, and persistent storage

The Docker Compose override defines MongoDB and Mongo Express. All services join the
Astro `airflow` network, allowing Airflow to reach MongoDB by the service name
`mongodb`. A named `mongo_data` volume is mounted at `/data/db`, preserving database
files across container restarts.

![Docker Compose override](screenshots/05-compose-override.png)

![Airflow home page after startup](screenshots/09-astro-start.png)

![Docker containers command output](screenshots/10-running-containers.png)

![Shared Docker network inspection output](screenshots/11-docker-network.png)

![MongoDB volume inspection output](screenshots/12-docker-volume.png)

## ETL DAG

The `currency_exchange_etl` DAG contains three tasks:

1. `extract_exchange_rates` requests the EUR rates from the Frankfurter v2 API and
   pushes them to XCom using the named key `raw_rates`.
2. `transform_exchange_rates` maps the API rows to the required fields and pushes the
   documents using the named key `transformed_rates`.
3. `load_to_mongodb` upserts each document into `currency_db.exchange_rates`. A
   unique compound index on base currency, target currency, and rate date prevents
   duplicate records when a run is retried or repeated.

Each stored document contains `base_currency`, `target_currency`, `exchange_rate`,
`rate_date`, `source`, and `retrieved_at`.

![DAG extract task and raw_rates XCom](screenshots/06-dag-extract.png)

![DAG transform task and transformed_rates XCom](screenshots/07-dag-transform.png)

![DAG load task](screenshots/08-dag-load.png)

## Execution results

The DAG was triggered in Airflow. All three task instances completed successfully.

![Airflow DAG list](screenshots/13-airflow-dag-list.png)

![Successful Airflow graph](screenshots/14-airflow-graph.png)

![Extract task log](screenshots/15-extract-log.png)

![Transform task log](screenshots/16-transform-log.png)

![Load task log](screenshots/17-load-log.png)

![Extract task raw_rates XCom](screenshots/18-raw-rates-xcom.png)

![Transform task transformed_rates XCom](screenshots/19-transformed-rates-xcom.png)

## MongoDB output and persistence

The successful run stored three EUR rate documents in MongoDB. After restarting the
MongoDB container, all three records remained available in the named volume.

![Mongo Express database list](screenshots/20-mongo-databases.png)

![Exchange rate collection](screenshots/21-exchange-rates.png)

![Expanded exchange rate document](screenshots/22-exchange-rate-document.png)

![MongoDB records after restarting the container](screenshots/23-persistence-proof.png)

## Source repository

The source code is available at: **[GitHub repository](https://github.com/lumia-Qcode/MLOPS_TASK_03)**

![Successful Git push output](screenshots/24-git-push.png)

![GitHub repository contents](screenshots/25-repository.png)

## Conclusion

The project demonstrates a complete Airflow ETL workflow using the Frankfurter API,
named XCom values, MongoDB, and Mongo Express. Docker networking enables service
discovery by container name, while the named MongoDB volume provides persistent
storage.
