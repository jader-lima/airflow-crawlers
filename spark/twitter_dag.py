from datetime import datetime
from os.path import join
from pathlib import Path

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import DAG
from airflow.operators.alura import TwitterOperator
from airflow.utils.dates import days_ago



ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(6),
}

BASE_FOLDER = join(
    str(Path("~/Documents").expanduser()),
    "alura/datapipeline/datalake/{stage}/twitter_aluraonline/{partition}",
)
PARTITION_FOLDER = "extract_date={{ ds }}"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

with DAG(
    dag_id="twitter_dag",
    default_args=ARGS,
    schedule_interval="0 9 * * *",
    max_active_runs=1
) as dag:
    spark_ingestion = SparkSubmitOperator(
        task_id="spark_ingestion",
        application=join(
            str(Path(__file__).parents[2]),
            "spark/ingestion.py"
        ),
        name="spark_ingestion",
        application_args=[
            "--src",
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "--dest",
            BASE_FOLDER.format(stage="silver", partition=""),
            "--process-date",
            "{{ ds }}",
        ]
    )
    #args.src, args.dest, args.table_name ,args.src_format, args.dest_format , args.options_dict,  args.process_date
    spark_ingestion
