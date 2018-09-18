import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

from operators.http_gcs import HttpToGcsOperator

dag = DAG(
    dag_id="my_first_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 6, 20),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)


def print_exec_date(**context):
    print(context["execution_date"])


my_task = PythonOperator(
    task_id="task_name", python_callable=print_exec_date, provide_context=True, dag=dag
)


query = """
    SELECT *
    FROM land_registry_price_paid_uk
    WHERE transfer_date = '{{ ds }}'
"""


pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="pgsl_to_gcs",
    postgres_conn_id="postgres_airflow_training",
    sql=query,
    bucket="airflow-training-data-tim",
    filename="land_registry_price_paid_uk/{{ ds }}/properties_{}.json",
    dag=dag,
)

for currency in {"EUR", "USD"}:
    HttpToGcsOperator(
        task_id="get_currency_" + currency,
        endpoint="airflow-training-transform-valutas?date={{ ds }}&from=GBP&to={cur}".format(cur=currency),
        bucket="airflow-training-data-tim",
        method="GET",
        http_conn_id="airflow-training-currency-http",
        gcs_conn_id="airflow-training-data-tim",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        dag=dag,
    )
