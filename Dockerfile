ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt