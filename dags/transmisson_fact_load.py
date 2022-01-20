########################################################################################
# This DAG submits tasks grouped on 
#  1. Stage Groups
#  
# Create a stage in such a manner that: 
# 1. Stage tables from different dbs can be loaded in one task group
# 2. Fact tables dependent on the stage tables are also loaded 
# (Fact tables dependent upon two different stages will not be loaded). 
# This gives little bit of flexibility over db based taskgroups
#######################################################################################
from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflowcommon import getBatchId, getpythonoperator, getbashoperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task_sensor import ExternalTaskSensor

# set the default config for the dag
dset = Variable.get("transmissionload", deserialize_json=True)
kinitparms = Variable.get("kinitparms", deserialize_json=True)
kinitprincipal = kinitparms["kinitprincipal"]
crpmdevicedict = dset["crpm_device_mapping"]
sparkjobs = crpmdevicedict["jobs"]
factdb = crpmdevicedict["factdb"]
srctoland = crpmdevicedict["src2land"]
land2stg = crpmdevicedict["land2stg"]
scriptpaths = dset["scriptpaths"]
kinitdomain=kinitparms["kinitdomain"]
edgenodehost = kinitparms["edgenodehost"]
password = Variable.get("kinit_passwd")


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

with DAG(dset["name"], default_args=default_args, schedule_interval=None, dagrun_timeout=timedelta(minutes=dset['dagrun_timeout'])) as dag:

    # Set the batch id from Airflow dag run
    setbatch = getpythonoperator("BatchId", getBatchId)

    batchid = "{{ ti.xcom_pull(key='batchId', task_ids='Run_BatchId') }}"

    
    # Running spark job
    command="base64 -d <<< {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "cd {} && sh {}".format(scriptpaths["cdcommand"],scriptpaths["sparkscript"]))
    
    taskname = "spark_transmission_run"
    ssh_spark = getbashoperator(taskname, False, command)

    # Task Group for Sqoop, validation and stage table load
  
    group =[]
    
    for stagegrp in sparkjobs:

        with TaskGroup(group_id="{}_SparktoFact".format(stagegrp)) as run_stage0:

            stagetaskgrp = []
            with TaskGroup(group_id="{}_SparktoStage".format(stagegrp)) as run_stage1:  
            
                for landtab in sparkjobs[stagegrp]["landtab"]:

                    dbname, tabname = landtab.split('.')
                    command="base64 -d <<< {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password, kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', land2stg[dbname], 'stage'))
                    taskname = "load_stage_{}_{}".format(dbname, tabname)
                    ssh_stage = getbashoperator(taskname, False, command)
                    ssh_stage
                    stagetaskgrp.append(run_stage1)

            facttaskgrp  = []
            with TaskGroup(group_id="{}_FactLoad".format(stagegrp)) as run_fact:

                for table in sparkjobs[stagegrp]["facttabs"]:
                    dbname, tabname = table.split('.')
                    command="base64 -d <<< {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', dbname, 'fact'))
                    taskname = "{}".format(tabname)
                    ssh_fact = getbashoperator(taskname, False, command)
                    facttaskgrp.append(run_fact)


            run_stage1 >> run_fact
        group.append(run_stage0) 

    dummyop1 = DummyOperator(task_id='NoOPFact1')
setbatch >>  ssh_spark >> group >> dummyop1


