FROM apache/airflow:2.9.2-python3.9

COPY airflow-requirements.txt /airflow-requirements.txt
RUN pip install --no-cache-dir -r /airflow-requirements.txt
