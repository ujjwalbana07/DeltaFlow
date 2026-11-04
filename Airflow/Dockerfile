FROM apache/airflow:3.2.2-python3.12
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
