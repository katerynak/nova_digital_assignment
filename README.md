# nova_digital_assignment

Instructions to run docker compose:
`docker compose -f docker-compose_airflow.yaml up -d`

Connections added through the Airflow interface:
1. Connection ID: titanic_http, conenction type: http, host: https://web.stanford.edu
2. Connection ID: my_sqlite_conn, connection type: sqlite, host: /opt/airflow/data/titanic.db
