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
import base64
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflowcommon import getBatchId, getpythonoperator, getbashoperator, getbashoperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.task_group import TaskGroup

# set the default config for the dag
dset = Variable.get("factloadjob1", deserialize_json=True)
kinitparms = Variable.get("kinitparms", deserialize_json=True)
password = kinitparms["kinitpass"]
password = base64.b64decode(password).decode('utf-8')
kinitprincipal = kinitparms["kinitprincipal"]
crpmdevicedict = dset["crpm_device_mapping"]
sqoopjobs = crpmdevicedict["jobs"]
factdb = crpmdevicedict["factdb"]
srctoland = crpmdevicedict["src2land"]
land2stg = crpmdevicedict["land2stg"]
scriptpaths = dset["scriptpaths"]
kinitprincipal = kinitparms["kinitprincipal"]
kinitdomain = kinitparms["kinitdomain"]
edgenodehost = kinitparms["edgenodehost"]

default_args = {
    'owner': dset['owner'],
    'start_date': days_ago(dset['days_ago']),
    'concurrency': dset['concurrency'],
    'retries': dset['retries']}

with DAG(dset["name"], default_args=default_args, schedule_interval=None, dagrun_timeout=timedelta(minutes=dset['dagrun_timeout'])) as dag:

    
    # Set the batch id from Airflow dag run
    setbatch = getpythonoperator("BatchId", getBatchId)

    batchid = "{{ ti.xcom_pull(key='batchId', task_ids='Run_BatchId') }}"
    # get the Kinit task
    command = 'echo {} | kinit {}'.format(password, kinitprincipal) 
    bash_kinit = getbashoperator("KinitEdge", False, command)

    # Task Group for Sqoop, validation and stage table load

    
    group =[]
    
    for stagegrp in sqoopjobs:

        with TaskGroup(group_id="{}_SqooptoFact".format(stagegrp)) as run_stage0:

            stagetaskgrp = []
            with TaskGroup(group_id="{}_SqooptoStage".format(stagegrp)) as run_stage1:  
            
                for landtab in sqoopjobs[stagegrp]["landtab"]:

                    dbname, tabname = landtab.split('.')

                    command="echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {}".format(scriptpaths["sqoop"], tabname, dbname))
                    taskname = "sqoop_{}_{}".format(dbname, tabname)
                    ssh_sqoop = getbashoperator(taskname, False, command)
					
                    command="echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password, kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {}".format(scriptpaths["baseval"], dbname, tabname, srctoland[dbname], 'true'))
                    taskname = "validation_{}_{}".format(dbname, tabname)
                    ssh_valid = getbashoperator(taskname, False, command)
					
                    command="echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password, kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', land2stg[dbname], 'stage'))
                    taskname = "load_stage_{}_{}".format(dbname, tabname)
                    ssh_stage = getbashoperator(taskname, False, command)

                    ssh_sqoop >> ssh_valid >> ssh_stage
                    stagetaskgrp.append(run_stage1)

            depstagetaskgrp = []
            with TaskGroup(group_id="{}_depstagetab".format(stagegrp)) as run_depstage:

                for stagedeptab in sqoopjobs[stagegrp]["depstage"]:

                    dbname, tabname = stagedeptab.split('.')
                    command="echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', dbname, 'stage'))
                    taskname = "load_depstage_{}_{}".format(dbname, tabname)
                    ssh_fact = getbashoperator(taskname, False, command)
                    depstagetaskgrp.append(run_depstage)

            filterruletaskgrp = []
            with TaskGroup(group_id="{}_Filterrule".format(stagegrp)) as run_filterrule:

                for filtertab in sqoopjobs[stagegrp]["filterrule"]:

                    dbname, tabname = filtertab.split('.')
                    command="echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', dbname, 'stage'))
                    taskname = "load_filter_{}".format(tabname)
                    ssh_fact = getbashoperator(taskname, False, command)
                    filterruletaskgrp.append(run_filterrule)


            facttaskgrp  = []
            with TaskGroup(group_id="{}_FactLoad".format(stagegrp)) as run_fact:

                for table in sqoopjobs[stagegrp]["facttabs"]:

                    command="echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', dbname, 'fact'))
                    taskname = "{}".format(table)
                    ssh_fact = getbashoperator(taskname, False, command)
                    facttaskgrp.append(run_fact)


            depfacttaskgrp  = []
            with TaskGroup(group_id="{}_DepFactLoad".format(stagegrp)) as run_depfact:

                for table in sqoopjobs[stagegrp]["depfact"]:

                    command="echo {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', dbname, 'fact'))
                    taskname = "{}".format(table)
                    ssh_fact = getbashoperator(taskname, False, command)
                    depfacttaskgrp.append(run_depfact)

            run_stage1 >> run_depstage >> run_filterrule >> run_fact >> run_depfact


        group.append(run_stage0) 

    dummyop1 = DummyOperator(task_id='NoOPFact1')

setbatch >> bash_kinit >> group >> dummyop1