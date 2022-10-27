FROM apache/airflow:2.3.0

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install yfinance && pip install pendulum