# nova_digital_assignment

Before you begin on Linux:
```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
For other OS check the Airflow website: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#initializing-environment

Instructions to run docker compose:
```bash
docker compose -f docker-compose_airflow.yaml up -d
```

Connections added through the Airflow interface:
1. Connection ID: titanic_http, conenction type: http, host: https://web.stanford.edu
2. Connection ID: my_sqlite_conn, connection type: sqlite, host: /opt/airflow/data/titanic.db
