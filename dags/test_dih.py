from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from sqlalchemy.sql.elements import Null
from airflowcommon import getpodoperator, getpythonoperator, getBatchId
import base64
from airflow.utils.task_group import TaskGroup

# Fetch config variables from Airflow
dset = Variable.get("snowflakedih", deserialize_json=True)

dihdict = dset["snowflake_dih_mapping"]
snowsqljobs = dihdict["jobs"]
factdb = dihdict["factdb"]
srctoland = dihdict["src2land"]
land2stg = dihdict["land2stg"]
scriptpaths = dset["scriptpaths"]

kinitparms = Variable.get("kinitparms", deserialize_json=True)
password = kinitparms["kinitpass"]
password = base64.b64decode(password).decode('utf-8')
kinitprincipal = kinitparms["kinitprincipal"]
kinitdomain = kinitparms["kinitdomain"]
edgenodehost = kinitparms["edgenodehost"]

kubeparams = Variable.get("kubeparams", deserialize_json=True)
labels={"job": "loaddihtables"}
namespace = kubeparams["namespace"]
image = kubeparams["image"]

# set the default config for the dag
default_args = {
    'owner': dset["owner"],
    #'depends_on_past': dset["depends_on_past"],
    'start_date': datetime.utcnow(),
    #'email': dset["email"],
    #'email_on_failure': dset["email_on_failure"],
    #'email_on_retry': dset["email_on_retry"],
    #'concurrency' : dset["concurrency"],
    #'retries': dset["retries"],
    #'retry_delay': timedelta(minutes=dset["retry_delay"])
}

group =[]


with DAG(dset["name"], default_args=default_args, schedule_interval=dset["schedule_interval"], dagrun_timeout=timedelta(minutes=dset["dagrun_timeout"])) as dag:

    dag.doc_md = __doc__  # providing that you have a docstring at the beggining of the DAG
    dag.doc_md = """
    This Dag runs the Day 2 jobs to load Hive Fact tables. This DAG runs on Kubernetes Cluster and makes use of Kubernetes Pod Operator to: <br />
    1. Kinits and SSH to Edge Node <br />
    2. Runs snowsql Job to expoort data out of Carelink snowflake server and dumps it on AWS S3  (landing zone) <br />
	3. Copy data from AWS S3 to HDFS
    3. Loads Hive Stage tables from landing zone  <br />
    4. Loads data to filter rule tables <br />
    5. Loads the Hive Fact tables <br />
    """  # otherwise, type it like this
    

    # Set the batch id from Airflow dag run
    setbatch = getpythonoperator("BatchId", getBatchId)
    batchid = "{{ ti.xcom_pull(key='batchId', task_ids='Run_BatchId') }}"

    for stagegrp in snowsqljobs:

        with TaskGroup(group_id="{}_S2F".format(stagegrp)) as run_stage0:

            stagetaskgrp = []
            with TaskGroup(group_id="{}_S2HS".format(stagegrp)) as run_stage1:

                for landtab in snowsqljobs[stagegrp]["table"]:

                    schemaname,dbname,tabname = landtab.split('.')
                    
                    #command="{} ; snowsql -a mdtplcprod.us-east-1 -u DEV_HILLTOPPERS_BI_SVC -d DEV_CDH_DB -s XDS_MAIN -w DEV_HILLTOPPERS_ANALYTICS_WH --private-key-path snowflake.pk -q  'select count(*) from {};'".format(expo,tabname)
                    taskname = "SF_{}_{}".format(schemaname, tabname)
                    taskid = 'TA_' + taskname
                    commands = "echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} -d {} -s {} -t {}".format(scriptpaths["snowexp"],dbname,schemaname,tabname))

                    ssh_dih = getpodoperator(namespace, image, commands, labels, taskname , taskid)
                    
                    taskname = "DISTCP_{}_{}".format(dbname, tabname)
                    taskid = 'TA_' + taskname
                    commands = "echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} -t {}".format(scriptpaths["distcp"], tabname))
                    ssh_distcp = getpodoperator(namespace, image, commands, labels, taskname , taskid)
                    
                    taskname = "STG_{}_{}".format(schemaname, tabname)
                    taskid = 'TA_' + taskname
                    commands = "echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password, kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', land2stg["dih"], 'stage'))
                    ssh_stage = getpodoperator(namespace, image, commands, labels, taskname, taskid)
                    
                    taskname = "CLR_{}_{}".format(schemaname, tabname)
                    taskid = 'TA_' + taskname
                    commands = "echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password, kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} -t {} -d {} ".format(scriptpaths["cleanup"], tabname , srctoland[dbname]))
                    ssh_cleanup = getpodoperator(namespace, image, commands, labels, taskname, taskid)
                    
            
                    ssh_dih >> ssh_distcp >> ssh_stage >> ssh_cleanup
            depstagetaskgrp = []
            with TaskGroup(group_id="{}_depstagetab".format(stagegrp)) as run_depstage:
                for stagedeptab in snowsqljobs[stagegrp]["depstage"]:
                    dbname, tabname = stagedeptab.split('.')
                    taskname = "DEPSTG_{}_{}".format(dbname, tabname)
                    taskid = 'TA_' + taskname
                    commands = "echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password, kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', dbname, 'stage'))
                    ssh_stage = getpodoperator(namespace, image, commands, labels, taskname, taskid)
                    depstagetaskgrp.append(run_depstage)
            # filterruletaskgrp = []
            # with TaskGroup(group_id="{}_Filterrule".format(stagegrp)) as run_filterrule:

                # for filtertab in snowsqljobs[stagegrp]["filterrule"]:

                    # dbname, tabname = filtertab.split('.')

                    # taskname = "FRL_{}_{}".format(dbname, tabname)
                    # taskid = 'TA_' + taskname
                    # commands = "echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        # kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', dbname, 'stage'))
                    # ssh_frule = getpodoperator(namespace, image, commands, labels, taskname, taskid)
                    # filterruletaskgrp.append(run_filterrule)


            facttaskgrp  = []
            with TaskGroup(group_id="{}_FactLoad".format(stagegrp)) as run_fact:

                for table in snowsqljobs[stagegrp]["facttabs"]:

                    
                    taskname = "FCT_{}".format(table)
                    taskid = 'TA_' + taskname
                    commands = "echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], table , batchid,  'dml', factdb, 'fact'))
                    ssh_fact = getpodoperator(namespace, image, commands, labels, taskname, taskid)
                    facttaskgrp.append(run_fact)


            # depfacttaskgrp  = []
            # with TaskGroup(group_id="{}_DepFactLoad".format(stagegrp)) as run_depfact:

                # for depfact in snowsqljobs[stagegrp]["depfact"]:

                    # dbname, tabname = depfact.split('.')
                    # taskname = "DEPFCT_{}".format(tabname)
                    # taskid = 'TA_'  + taskname
                    # commands = "echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        # kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', dbname, 'fact'))
                    # ssh_depfact = getpodoperator(namespace, image, commands, labels, taskname, taskid)
                    # depfacttaskgrp.append(run_depfact)
            run_stage1 >> run_depstage >> run_fact #>> run_filterrule >> run_fact >> run_depfact
        group.append(run_stage0)
    dummyop1 = DummyOperator(task_id='DIHLODCMP')
setbatch >> group >> dummyop1