import datetime

from airflow import models
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

DAG_NAME = 'test_pod_operator'
args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval="@once",
    tags=['example']
)

quay_k8s = KubernetesPodOperator(
    namespace='default',
    image='quay.io/apache/bash',
    image_pull_secrets=[k8s.V1LocalObjectReference('testquay')],
    cmds=["bash", "-cx"],
    arguments=["echo", "10", "echo pwd"],
    labels={"foo": "bar"},
    name="airflow-private-image-pod",
    is_delete_operator_pod=True,
    in_cluster=True,
    task_id="task-two",
    get_logs=True,
    )