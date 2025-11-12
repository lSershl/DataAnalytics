import logging
import duckdb
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "user"
DAG_ID = "raw_from_api_to_s3"

# Используемые таблицы в DAG
BUCKET = "storage"
LAYER = "raw"
SOURCE = "earthquake"

# Ключи Minio S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# Краткое описание DAG (по желанию)
SHORT_DESCRIPTION = "SHORT DESCRIPTION"

# Детальное описание DAG (по желанию)
LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    end_date = pendulum.parse(end_date).add(days=1).format("YYYY-MM-DD")

    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):
    """"""

    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Loading started for dates: {start_date} / {end_date}")
    con = duckdb.connect()

    con.sql(
        f"""
        SET TIMEZONE = 'UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY
        (
            SELECT
                *
            FROM
                read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') AS res
        ) TO 's3://{BUCKET}/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';

        """,
    )

    con.close()
    logging.info(f"✅ Download successful for date: {start_date}")


with DAG(
    dag_id = DAG_ID,
    schedule = "0 5 * * *", # cron выражение, задаёт периодичность (в данном случае: каждый день в 5 утра)
    default_args = args,
    tags = ["s3", "raw"],
    description = SHORT_DESCRIPTION,
    max_active_tasks = 1,
    max_active_runs = 1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id = "start",
    )

    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id = "get_and_transfer_api_data_to_s3",
        python_callable = get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id = "end",
    )

    start >> get_and_transfer_api_data_to_s3 >> end