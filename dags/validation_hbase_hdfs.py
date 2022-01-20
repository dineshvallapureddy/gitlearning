########################################################################################
# This DAG will trigger validation script 
# 1.  
# 2. 
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
dset = Variable.get("hbasetohdfsvalidation", deserialize_json=True)
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
    #command="base64 -d <<< {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "cd {} && sh {}".format(scriptpaths["cdcommand"],scriptpaths["sparkscript"]))
	for tabconf in confs:
		command="base64 -d <<< {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password, kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "cd {} && sh {} -c {}".format(scriptpaths["cdcommand"],scriptpaths["validationscript"],tabconf))
    
		taskname = "validation_hbase_hdfs"
		validation = getbashoperator(taskname, False, command)
	dummyop1 = DummyOperator(task_id='NoOP')
setbatch >>  ssh_spark >> dummyop1