## Currency Exchange ETL

This Astro/Airflow project fetches EUR exchange rates for USD, GBP, and JPY from the
[Frankfurter API](https://api.frankfurter.dev), reshapes each rate, and upserts the
documents into MongoDB. Named XCom keys (`raw_rates` and `transformed_rates`)
transfer the data between the three tasks.

### Services

- Airflow UI: <http://localhost:8080>
- Mongo Express: <http://localhost:8081> (use credentials from your local `.env`)
- MongoDB: `mongodb:27017` inside the Astro Docker network

MongoDB stores its files in the named `mongo_data` volume, so the data persists
across container restarts and `astro dev stop`.

### Run locally

Install Docker Desktop, WSL 2, Git, and the Astro CLI. Copy `.env.example` to
`.env` and `airflow_settings.example.yaml` to `airflow_settings.yaml`, then set the
same MongoDB credentials in both files. The real `.env` and
`airflow_settings.yaml` files are ignored by Git. In PowerShell, load the Compose
variables into the current terminal before starting Astro:

```powershell
Get-Content .env | ForEach-Object {
    $name, $value = $_ -split '=', 2
    Set-Item "Env:$name" $value
}
astro dev start
```

In the Airflow UI, enable and trigger `currency_exchange_etl`. The DAG writes
documents to `currency_db.exchange_rates`. Stop the local environment with:

```powershell
astro dev stop
```

The local MongoDB and Mongo Express services are defined in
`docker-compose.override.yml`; the DAG dependencies are listed in
`requirements.txt`.
