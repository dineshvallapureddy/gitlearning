from airflow.example_dags.subdags.subdag import subdag
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago

DAG_NAME = 'example_subdag_operator'

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

start = DummyOperator(
    task_id='start',
    dag=dag,
)

section_1 = SubDagOperator(
    task_id='section-1',
    subdag=subdag(DAG_NAME, 'section-1', args),
    dag=dag,
)

some_other_task = DummyOperator(
    task_id='some-other-task',
    dag=dag,
)

section_2 = SubDagOperator(
    task_id='section-2',
    subdag=subdag(DAG_NAME, 'section-2', args),
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> section_1 >> some_other_task >> section_2 >> end