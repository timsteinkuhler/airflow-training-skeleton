import datetime as dt

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator
)

from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

from operators.http_gcs import HttpToGcsOperator

PROJECT_ID = "gdd-4144b215cd4d2ac616172ab57d"

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


dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="dataproc_delete_cluster",
    cluster_name="analyse-pricing-{{ ds }}",
    dag=dag,
    project_id=PROJECT_ID,
)


dataproc_compute_aggregates = DataProcPySparkOperator(
    task_id="dataproc_compute_aggregates",
    main="file://../helpers/build_statistics.py",
    cluster_name="analyse-pricing-{{ ds }}",
    arguments=["{{ ds }}"],
    dag=dag,
)


dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="dataproc_create_cluster",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id=PROJECT_ID,
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
    auto_delete_ttl=5 * 60,  # Autodelete after 5 minutes
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
) >> dataproc_create_cluster


for currency in {"EUR", "USD"}:
    HttpToGcsOperator(
        task_id="get_currency_" + currency,
        endpoint="airflow-training-transform-valutas?date={{ ds }}&from=GBP&to={cur}".format(
            cur=currency
        ),
        bucket="airflow-training-data-tim",
        method="GET",
        http_conn_id="airflow-training-currency-http",
        gcs_conn_id="airflow-training-data-tim",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        dag=dag,
    ) >> dataproc_create_cluster

dataproc_create_cluster >> dataproc_compute_aggregates >> dataproc_delete_cluster