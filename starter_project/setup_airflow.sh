#!/usr/bin/env bash
set -euo pipefail

# 1) Venv
python3 -m venv .venv
source .venv/bin/activate

# 2) Cài Airflow
export AIRFLOW_VERSION=2.9.3
export PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# 3) Init DB
export AIRFLOW_HOME="$PWD/airflow_home"
airflow db init

# 4) Tạo user admin
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin

# 5) Copy DAG
mkdir -p "$AIRFLOW_HOME/dags"
cp starter_project/dags/sales_data_quality_pipeline.py "$AIRFLOW_HOME/dags/"

echo "Chạy scheduler và webserver ở 2 terminal:"
echo "  airflow scheduler"
echo "  airflow webserver --port 8080"