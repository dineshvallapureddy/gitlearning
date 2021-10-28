[api]
auth_backend = airflow.api.auth.backend.deny_all

[celery]
worker_concurrency = 16

[celery_kubernetes_executor]
kubernetes_queue = kubernetes

[core]
colored_console_log = False
dags_folder = /opt/airflow/dags/repo//dags
executor = CeleryExecutor
load_examples = False
remote_logging = False
logging_level = WARN

[elasticsearch]
json_format = True
log_id_template = {dag_id}_{task_id}_{execution_date}_{try_number}

[elasticsearch_configs]
max_retries = 3
retry_timeout = True
timeout = 30

[kerberos]
ccache = /var/kerberos-ccache/cache
keytab = /etc/airflow.keytab
principal = airflow@FOO.COM
reinit_frequency = 3600

[kubernetes]
airflow_configmap = airflow-airflow-config
airflow_local_settings_configmap = airflow-airflow-config
multi_namespace_mode = False
namespace = airflow
pod_template_file = /opt/airflow/pod_templates/pod_template_file.yaml
worker_container_repository = apache/airflow
worker_container_tag = 2.1.4

[logging]
colored_console_log = False
remote_logging = False

[metrics]
statsd_host = airflow-statsd
statsd_on = True
statsd_port = 9125
statsd_prefix = airflow

[scheduler]
run_duration = 41460
statsd_host = airflow-statsd
statsd_on = True
statsd_port = 9125
statsd_prefix = airflow

[webserver]
enable_proxy_fix = True
rbac = True