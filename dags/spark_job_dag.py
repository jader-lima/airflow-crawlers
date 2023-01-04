import airflow
from datetime import timedelta
from os.path import join
from pathlib import Path
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
}

ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(6),
}
spark_master = "spark://spark:7077"

TABLE_NAME = 'teste'

SRC_FORMAT = 'csv'

DEST_FORMAT = 'parquet'
#    str(Path("~/Documents").expanduser()),

#SRC_FOLDER = join(
#    str(Path("./data/").expanduser()),
#    "../spark-data/datalake/{stage}/{file}",
#)

#BASE_FOLDER = join(
#    str(Path("./data/").expanduser()),
#    "../spark-data/datalake/{stage}/teste/{partition}",
#)

SRC_FOLDER = join(
    "/opt/spark-data/datalake/{stage}/{file}",
)

BASE_FOLDER = join(
    "/opt/spark-data/datalake/{stage}/teste/{partition}",
)


PARTITION_FOLDER = "extract_date={{ ds }}"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

spark_dag = DAG(
    dag_id="spark_job_dag",
    default_args=ARGS,
    schedule_interval="0 9 * * *",
    max_active_runs=1
)

spark_ingestion = SparkSubmitOperator(
        task_id="spark_ingestion",
        application=join(
            str(Path(__file__).parents[2]),
            "spark/ingestion/ingestion.py"
        ),
        dag=spark_dag,
        name="spark_ingestion",
        conn_id="spark_default",
        verbose=1,
        application_args=[
            "--src",
            SRC_FOLDER.format(stage="transient", file="teste"),
            "--dest",
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "--table_name",
            TABLE_NAME,            
            "--src_format",
            SRC_FORMAT,
            "--dest_format",
            DEST_FORMAT,
            "--process_date",
            "{{ ds }}"
            ]
    )
spark_ingestion

 #./bin/spark-submit --master spark://172.23.0.2:7077 --name spark_ingestion --verbose ~/Documentos/projetos/airflow-crawlers/spark/ingestion.py 
 #--src ~/datapipeline/datalake/transient/teste/ 
 #--dest ~/datapipeline/datalake/ 
 #--table_name teste 
 #--src_format csv 
 #--dest_format parquet 
 #--process-date 2022-11-15

#Path does not exist: file:/opt/***/spark-data/datalake/transient/teste
#park-submit --master spark://172.21.0.5:7077 --name spark_ingestion --verbose /opt/spark/ingestion/ingestion.py -
# -src data/../spark-data/datalake/transient/teste/extract_date=2022-12-25 
# --destat csv 
# --dest_for data/../spark-data/datalake/bronze/teste/extract_date=2022-12-25 
# --table_name teste 
# --src_formmat parquet 
# --process_date 2022-12-25.


