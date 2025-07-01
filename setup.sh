# #Docker Compose Down
docker compose -f ./setup/monitoring_setup/docker-compose.yml down -v
docker compose -f ./setup/postgres_dwh/docker-compose.yml down -v
docker compose -f ./setup/postgres_src/docker-compose.yml down -v

docker compose -f ./setup/airflow/docker-compose.yml down -v
docker compose -f ./setup/logging_setup/docker-compose.yml down -v


# Start MinIO
docker compose -f ./setup/logging_setup/docker-compose.yml up --build -d

# Start Airflow
docker compose -f ./setup/airflow/docker-compose.yml up --build -d

#Start Monitoring
docker compose -f ./setup/monitoring_setup/docker-compose.yml up --build -d

# Start Postgres
docker compose -f ./setup/postgres_src/docker-compose.yml up --build -d
# Start Postgres
docker compose -f ./setup/postgres_dwh/docker-compose.yml up --build -d


#Import Conn and Variables
docker exec -it airflow-webserver airflow connections import /opt/airflow/conn_var/conn.yaml
docker exec -it airflow-webserver airflow variables import  /opt/airflow/conn_var/var.json


