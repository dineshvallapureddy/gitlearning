from airflow import DAG
import base64
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.plugins_manager import AirflowPlugin
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
#from src.main.python.airflowhelper import getBatchId
import re
import paramiko
import gssapi

##############################################################################################
## Set Airflow default variables
#############################################################################################
kinitparms = Variable.get("kinitparms", deserialize_json=True)
edgenodehost = kinitparms["edgenodehost"]



##############################################################################################
## A Custom SSH hook to enable Authentication using Kerberos. This is not enabled in SSHOperator 
## by default
##############################################################################################
class CustomSSHHook(SSHHook):
    """
    Custom SSH Hook with kerberose authentication support
    """

    def __init__(self,
                 ssh_conn_id=None,
                 remote_host=None,
                 username=None,
                 password=None,
                 key_file=None,
                 port=None,
                 timeout=10,
                 keepalive_interval=30):
        super(CustomSSHHook, self).__init__(
                 ssh_conn_id,
                 remote_host,
                 username,
                 password,
                 key_file,
                 port,
                 timeout,
                 keepalive_interval)
        # Use connection to override defaults
        self.gss_auth = False
        if self.ssh_conn_id is not None:
            conn = self.get_connection(self.ssh_conn_id)

            if conn.extra is not None:
                extra_options = conn.extra_dejson
                if "gss_auth" in extra_options \
                        and str(extra_options["gss_auth"]).lower() == 'true':
                    print("IN GSS AUTH")
                    self.gss_auth = True

    def get_conn(self):
        """
                Opens a ssh connection to the remote host.

                :return paramiko.SSHClient object
                """

        self.log.debug('Creating SSH client for conn_id: %s', self.ssh_conn_id)
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        if self.no_host_key_check:
            # Default is RejectPolicy
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if self.password and self.password.strip():
            client.connect(hostname=self.remote_host,
                           username=self.username,
                           password=self.password,
                           key_filename=self.key_file,
                           timeout=self.timeout,
                           compress=self.compress,
                           port=self.port,
                           sock=self.host_proxy)
        else:
            print("IAM HERE!!", self.remote_host,self.username )
            client.connect(hostname=self.remote_host,
                           username=self.username,
                           key_filename=None,
#                           key_filename=self.key_file,
                           timeout=self.timeout,
                           compress=self.compress,
                           port=22,
#                           port=self.port,
#                           sock=self.host_proxy,
                           gss_auth=True,
                           gss_kex=True,
                           gss_deleg_creds=True
                           )

        if self.keepalive_interval:
            client.get_transport().set_keepalive(self.keepalive_interval)

        self.client = client
        return client



class CustomSshPlugin(AirflowPlugin):
    name = "ssh_plugins"
    DEFAULT_PLUGIN = "1.0"
    hooks = [CustomSSHHook]



def getedgenodehook():
    return CustomSSHHook(ssh_conn_id="sharedsvcdev3-edge",
            remote_host=edgenodehost,
            port=22,
            timeout=100)

#############################################################################################
# Get the Batch ID from DAG run - Batch Id is a timestamp without any separators. Batch Id is 
# passed to downstream HQL task through XCOM
#############################################################################################
def getBatchId(**kwargs):
    context = kwargs
    dagrunid = context['dag_run'].run_id
    pattern = re.compile('\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')
    tstamp = pattern.findall(dagrunid)[0]
    batchId = tstamp.replace("-", "").replace("T", ""). replace(":", "")
    print(f'Setting BATCH_ID to {batchId}')
    kwargs['ti'].xcom_push(key='batchId', value=batchId)

##############################################################################################
## Common template for Pythonoperator 
##############################################################################################
#def getpythonoperator(taskname, callable, dag):
def getpythonoperator(taskname, callable):
    return PythonOperator(
        task_id="Run_{}".format(taskname),
        provide_context=True,
        python_callable=callable)
#        dag=dag)

##############################################################################################
## Common template for Bash Operator 
##############################################################################################
#def getbashoperator(taskname, xcompush, dag, command):
def getbashoperator(taskname, xcompush, command):
    return BashOperator(
        task_id="Run_{}".format(taskname),
        bash_command=command,
        do_xcom_push=xcompush)
#        dag=dag
#        )

##############################################################################################
## Common template for SSH Operator 
##############################################################################################
#def getsshoperator(taskname, xcompush, dag, command ):
def getsshoperator(taskname, xcompush, command):
    return SSHOperator(
        ssh_hook=getedgenodehook(), 
        task_id="Run_{}".format(taskname),
        do_xcom_push=xcompush,
        command=command
        )

##############################################################################################
## Common template for Kubernetes pod Operator 
##############################################################################################
#def getsshoperator(taskname, xcompush, dag, command ):
def getpodoperator(namespace, image, commands, labels, taskname, taskid):
    return KubernetesPodOperator(namespace=namespace, 
        image=image,
        cmds=["bash", "-cx"],
        arguments=[commands],
        labels=labels,
        name=taskname,
        task_id=taskid,
        get_logs=True,
        do_xcom_push=False
        )