import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)


# Так делать не хорошо, но мы аккуратно
# Все секреты надо хранить в секрете)
YC_DP_AZ = 'ru-central1-d'
YC_DP_SSH_PUBLIC_KEY = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIARMRnI4TdE5aRv7Zvm4ePEMb8KSY3vyKexA1j0fXDPn andre@DESKTOP-JTR14DV'
YC_DP_SUBNET_ID = 'fl8j2ck4ljd6o0othphi'
YC_DP_SA_ID = 'ajeomd09a1mhf246icat'
YC_DP_METASTORE_URI = '10.131.0.9'
YC_SOURCE_BUCKET = 'mentor-hw'
YC_DP_LOGS_BUCKET = 'mentor-hw'


with DAG(
        'my-final-hw-dag',
        schedule_interval='@hourly',
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Временный кластер для выполнения PySpark-задания под оркестрацией Managed Service for Apache Airflow™',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_DP_LOGS_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-ssd',
        masternode_disk_size=60,
        computenode_resource_preset='m2.large',
        computenode_disk_type='network-ssd',
        computenode_disk_size=60,
        computenode_count=1,
        computenode_max_hosts_count=1,
        services=['YARN', 'SPARK'],
        datanode_count=0,
        properties={
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
    )

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/scripts/prepare_fraud_info.py',
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster
