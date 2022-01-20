from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflowcommon import getpythonoperator, getBatchId,getbashoperator
from airflow.utils.task_group import TaskGroup

# Fetch config variables from Airflow
dset = Variable.get("createhivetable", deserialize_json=True)

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
    This Dag creates all hive stage,filter,base(DIH) and fact tables. 
    1. Creates all tables in stage databases
        crpm_clinic_stg
        crpm_device_stg
        crpm_dmfservices_stg
        crpm_fdn_stg
        crpm_instrumentation_stg
        dih_stg
    2. Creates all tables in filter database.
        linqdm_filter
    3. Creates all tables in Fact database.
        linqdm_fdn
    4. Creates all tables in Base database for now this database will have only DIH tables.
        dih
       """  
    
    # Set the batch id from Airflow dag run
    setbatch = getpythonoperator("BatchId", getBatchId)
    batchid = "{{ ti.xcom_pull(key='batchId', task_ids='Run_BatchId') }}"

    for db in database:

        with TaskGroup(group_id="{}_Tab".format(db)) as run_stage0:
            stagetaskgrp = []
            with TaskGroup(group_id="{}_S2HS".format(db)) as run_stage1:
                for tabname in database[db]["tabname"]:

                    taskname = "CRT_{}_{}".format(db, tabname)
                    taskid = 'TA_' + taskname
                    commands = "base64 -d <<< {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -oGSSAPIDelegateCredentials=yes {}@{} '{}'".format(password, kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {}  {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid, 'ddl', db, database[db]["type"]))
                    ssh_create_stage = getbashoperator(taskname, False, commands)
                    ssh_create_stage
                    stagetaskgrp.append(run_stage1)
            run_stage1
        group.append(run_stage0) 

    dummyop = DummyOperator(task_id='NoOP')

setbatch >> group >> dummyop
    