import os
import sys

from datetime import datetime, timedelta, date

from airflow.models import DAG
from airflow.operators import PythonOperator

from common.operators.dataproc_operator import DataProcHiveOperator, DataprocClusterCreateOperator, DataprocClusterDeleteOperator

persist_cfg = retrieve_persist_cfg()
dag_args = persist_cfg['dag_args']

args = {
    'owner': dag_args['owner'],
    'depends_on_past': False,
    'start_date': datetime(2019, 06, 26),
    'retries': dag_args['retries'],
    'retry_delay': timedelta(minutes=dag_args["retry_delay_minutes"]),
    'email': dag_args['email'],
    'email_on_failure': dag_args['email_on_failure'],
    'email_on_retry': dag_args['email_on_retry']
}

on_demand_dataproc_dag = DAG(dag_id = "on_demand_dataproc_dag", default_args = args, schedule_interval = "0 3 * * *")


dataproc_hive_dict = {
    "example_hive_job":{"hql": exmaple_hive_job_hql, "operator": DataProcHiveOperator(task_id="none", dataproc_cluster="none", region="none", gcp_conn_id="none", query="none")},
}

for op_name in dataproc_hive_dict:
    dataproc_hive_dict[op_name]["operator"] = DataProcHiveOperator(
        task_id = op_name,
        dataproc_cluster = """hadoop-cluster-kpi-pacing-ratio-{{ ds }}""",
        region = persist_cfg["gcp_prod_project"]["region"],
        gcp_conn_id = persist_cfg["gcp_prod_project"]["gcp_conn_id"],
        query = persist_cfg[op_name]["hql"],
        scheduler = "yarn",
        queue = persist_cfg["queue"],
        dag = on_demand_dataproc_dag
    )

# explore on-demand dataproc hadoop cluster
# should use execution_date in cluster name,
# otherwise it may happend that the today dates will be different for different tasks
# for dags with lots of tasks
create_dataproc_cluster = DataprocClusterCreateOperator(
    task_id = "create_dataproc_cluster",
    cluster_name = """hadoop-cluster-kpi-pacing-ratio-{{ ds }}""",
    project_id = persist_cfg["gcp_prod_project"]["project_id"],
    region = persist_cfg["gcp_prod_project"]["region"],
    zone = persist_cfg["gcp_prod_project"]["zone"],
    google_cloud_conn_id = persist_cfg["gcp_prod_project"]["gcp_conn_id"],
    subnetwork = persist_cfg["gcp_subnetwork"][persist_cfg["gcp_prod_project"]["region"]],
    internal_ip_only = True,
    image_version = "1.2.65-deb9",
    storage_bucket = persist_cfg["gcp_prod_project"]["storage_bucket"],
    init_actions_uris = persist_cfg["gcp_prod_project"]["dataproc_config"]["init_actions_uris"],
    metadata = persist_cfg["gcp_prod_project"]["dataproc_config"]["metadata"],
    properties = persist_cfg["gcp_prod_project"]["dataproc_config"]["properties"],
    master_machine_type='n1-standard-4',
    master_disk_size=500,
    num_workers = 2,
    worker_machine_type='n1-standard-4',
    worker_disk_size=500,
    num_preemptible_workers=2,
    queue = persist_cfg["queue"],
    dag = on_demand_dataproc_dag
)

delete_dataproc_cluster = DataprocClusterDeleteOperator(
    task_id = "delete_dataproc_cluster",
    cluster_name = """hadoop-cluster-kpi-pacing-ratio-{{ ds }}""",
    project_id = persist_cfg["gcp_prod_project"]["project_id"],
    google_cloud_conn_id = persist_cfg["gcp_prod_project"]["gcp_conn_id"],
    region = persist_cfg["gcp_prod_project"]["region"],
    queue = persist_cfg["queue"],
    dag = on_demand_dataproc_dag
)

create_dataproc_cluster.set_upstream(get_param_dates)
dataproc_hive_dict["example_hive_job"]["operator"].set_upstream(create_dataproc_cluster)
delete_dataproc_cluster.set_upstream(dataproc_hive_dict["example_hive_job"]["operator"])


#
