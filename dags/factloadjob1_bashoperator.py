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
from airflow.models import Variable
from airflowcommon import getBatchId, getpythonoperator, getbashoperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

# set the default config for the dag
dset = Variable.get("factloadjob1", deserialize_json=True)
kinitparms = Variable.get("kinitparms", deserialize_json=True)
kinitprincipal = kinitparms["kinitprincipal"]
crpmdevicedict = dset["crpm_device_mapping"]
sqoopjobs = crpmdevicedict["jobs"]
factdb = crpmdevicedict["factdb"]
srctoland = crpmdevicedict["src2land"]
land2stg = crpmdevicedict["land2stg"]
scriptpaths = dset["scriptpaths"]
kinitdomain = kinitparms["kinitdomain"]
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

    # Task Group for Sqoop, validation and stage table load

    
    group =[]
    
    for stagegrp in sqoopjobs:

        with TaskGroup(group_id="{}_SqooptoFact".format(stagegrp)) as run_stage0:

            stagetaskgrp = []
            with TaskGroup(group_id="{}_SqooptoStage".format(stagegrp)) as run_stage1:  
            
                for landtab in sqoopjobs[stagegrp]["landtab"]:

                    dbname, tabname = landtab.split('.')

                    command="base64 -d <<< {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {}".format(scriptpaths["sqoop"], tabname, dbname))
                    taskname = "sqoop_{}_{}".format(dbname, tabname)
                    ssh_sqoop = getbashoperator(taskname, False, command)
										
                    command="base64 -d <<< {}| kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password, kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', land2stg[dbname], 'stage'))
                    taskname = "load_stage_{}_{}".format(dbname, tabname)
                    ssh_stage = getbashoperator(taskname, False, command)

                    ssh_sqoop  >> ssh_stage
                    stagetaskgrp.append(run_stage1)

            depstagetaskgrp = []
            with TaskGroup(group_id="{}_depstagetab".format(stagegrp)) as run_depstage:

                for stagedeptab in sqoopjobs[stagegrp]["depstage"]:

                    dbname, tabname = stagedeptab.split('.')
                    command="base64 -d <<< {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', dbname, 'stage'))
                    taskname = "load_depstage_{}_{}".format(dbname, tabname)
                    ssh_fact = getbashoperator(taskname, False, command)
                    depstagetaskgrp.append(run_depstage)

            filterruletaskgrp = []
            with TaskGroup(group_id="{}_Filterrule".format(stagegrp)) as run_filterrule:

                for filtertab in sqoopjobs[stagegrp]["filterrule"]:

                    dbname, tabname = filtertab.split('.')
                    command="base64 -d <<< {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', dbname, 'stage'))
                    taskname = "load_filter_{}".format(tabname)
                    ssh_fact = getbashoperator(taskname, False, command)
                    filterruletaskgrp.append(run_filterrule)


            facttaskgrp  = []
            with TaskGroup(group_id="{}_FactLoad".format(stagegrp)) as run_fact:

                for facttab in sqoopjobs[stagegrp]["facttabs"]:
                    dbname, tabname = facttab.split('.')
                    command="base64 -d <<< {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', dbname, 'fact'))
                    taskname = "{}".format(tabname)
                    ssh_fact = getbashoperator(taskname, False, command)
                    facttaskgrp.append(run_fact)


            depfacttaskgrp  = []
            with TaskGroup(group_id="{}_DepFactLoad".format(stagegrp)) as run_depfact:

                for depfact in sqoopjobs[stagegrp]["depfact"]:
                    dbname, tabname = depfact.split('.')
                    command="base64 -d <<< {} | kinit {}@{} && ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes {}@{} '{}'".format(password,
                        kinitprincipal, kinitdomain, kinitprincipal, edgenodehost, "{} {} {} {} {} {}".format(scriptpaths["hiveload"], tabname , batchid,  'dml', dbname, 'fact'))
                    taskname = "{}".format(tabname)
                    ssh_fact = getbashoperator(taskname, False, command)
                    depfacttaskgrp.append(run_depfact)

            run_stage1 >> run_depstage >> run_filterrule >> run_fact >> run_depfact


        group.append(run_stage0) 

    dummyop1 = DummyOperator(task_id='NoOPFact1')

setbatch >> group >> dummyop1