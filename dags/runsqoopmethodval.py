from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflowcommon import getbashoperator, getpythonoperator, getBatchId
import base64
from airflow.utils.task_group import TaskGroup

# Fetch config variables from Airflow
dset = Variable.get("methodvalsqoop", deserialize_json=True)

scriptpaths = dset["scriptpaths"]
database = dset["database"]
kinitparms = Variable.get("kinitparms", deserialize_json=True)
kinitprincipal = kinitparms["kinitprincipal"]
kinitdomain = kinitparms["kinitdomain"]
edgenodehost = kinitparms["edgenodehost"]
password = Variable.get("kinit_passwd")
# set the default config for the dag
default_args = {
    'owner': dset["owner"],
    'depends_on_past': dset["depends_on_past"],
    'start_date': datetime.utcnow(),
    'email': dset["email"],
    'email_on_failure': dset["email_on_failure"],
    'email_on_retry': dset["email_on_retry"],
    'concurrency' : dset["concurrency"],
    'retries': dset["retries"],
    'retry_delay': timedelta(minutes=dset["retry_delay"])
}

group =[]

with DAG(dset["name"], default_args=default_args, schedule_interval=dset["schedule_interval"], dagrun_timeout=timedelta(minutes=dset["dagrun_timeout"])) as dag:

    dag.doc_md = __doc__  # providing that you have a docstring at the beggining of the DAG
    dag.doc_md = """
    This Dag runs the method validation for all the SQQOP jobs. This runs after all the fact tables are loaded. This job performs rowcount match and data mismatch checks. The results are stored in a hive table for further analysis. """  
    
    # Set the batch id from Airflow dag run
    setbatch = getpythonoperator("BatchId", getBatchId)
    batchid = "{{ ti.xcom_pull(key='batchId', task_ids='Run_BatchId') }}"

    for db in database:

        with TaskGroup(group_id="{}_Db".format(db)) as run_stage0:
            stagetaskgrp = []
            
            with TaskGroup(group_id="{}_MVAL".format(db)) as run_stage1:
                for tabname in database[db]["tables"]:

                    taskname = "MVAL_{}_{}".format(db, tabname)
                    taskid = 'TA_' + taskname
                    command="base64 -d <<< {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password, kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {}".format(scriptpaths["baseval"], db, tabname, dset["src2land"][db], batchid))
                    ssh_valid = getbashoperator(taskname, False, command)

                    ssh_valid
                    stagetaskgrp.append(run_stage1)
            run_stage1
        group.append(run_stage0) 

    dummyop = DummyOperator(task_id='NoOP')

setbatch >> group >> dummyop
    