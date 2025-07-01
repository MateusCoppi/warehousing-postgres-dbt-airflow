FROM apache/airflow:2.10.5

COPY requirements.txt /

USER root

RUN mkdir -p /opt/airflow/dbt/logs && \
    chown -R airflow: /opt/airflow/dbt && \
    chmod -R 755 /opt/airflow/dbt

USER airflow

RUN pip install -r /requirements.txt




