FROM apache/airflow:2.8.4
COPY requirements.txt .
RUN pip install -r requirements.txt


USER root
RUN mkdir -p /opt/airflow/data
COPY data/ /opt/airflow/data/

RUN chown -R airflow:root /opt/airflow/data
USER airflow