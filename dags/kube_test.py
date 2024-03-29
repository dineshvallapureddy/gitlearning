import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import models
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

DAG_NAME = 'test_pod_operator'
args = {
    'owner': 'Airflow',
   
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval="@once",
    tags=['test']
)

quay_k8s = KubernetesPodOperator(
    namespace='airflow',
    image='kunal627/snowsql',
    #image_pull_secrets=[k8s.V1LocalObjectReference('testquay')],
    cmds=["bash", "-cx"],
    arguments=["echo", "10", "echo pwd"],
    name="airflow-private-image-pod",
    is_delete_operator_pod=True,
    in_cluster=True,
    task_id="task-two",
    get_logs=True,
    )
    
    
quay_k8s
    
