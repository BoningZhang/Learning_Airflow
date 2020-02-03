import os
import sys
import logging

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators import BashOperator, PythonOperator, SubDagOperator

from sync_data_to_gcp.tasks.hqls.export_queries import *
from common.operators.export_cdc_to_gcp_dag import export_to_gcp_dag

persist_cfg = retrieve_persist_cfg()
dag_args = persist_cfg['dag_args']

args = {
    'owner': dag_args['owner'],
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 30),
    'retries': dag_args['retries'],
    'retry_delay': timedelta(minutes=dag_args["retry_delay_minutes"]),
    'email': dag_args['email'],
    'email_on_failure': dag_args['email_on_failure'],
    'email_on_retry': dag_args['email_on_retry']
}

sync_data2GCP_dag = DAG(dag_id="sync_data2GCP_dag",
                    default_args=args,
                    schedule_interval="0 3 * * *",
                    max_active_runs=2)


export_table_dict = {
    "export_table1_to_gcp": {
        "operator": None,
        "hql_dict": {
            "export_data_hql": export_table1_to_gcp_hql,
            "add_dataproc_partition_hql": add_dataproc_table1_partition_hql,
            "drop_tmp_table_hql": drop_tmp_table1_hql
        }
    },
    "export_table2_to_gcp": {
        "operator": None,
        "hql_dict": {
            "export_data_hql": export_table2_to_gcp_hql,
            "add_dataproc_partition_hql": add_dataproc_table2_partition_hql,
            "drop_tmp_table_hql": drop_tmp_table2_hql
        }
    }
}

export_table_params = {
    'gcp_warehouse_path': persist_cfg['gcp_warehouse']['casesci_prd'],
    'gcp_keyfile' : persist_cfg['gcp_keyfile']['casesci_prd']
}

export_table_dataproc_config = {
    "dataproc_cluster": persist_cfg["gcp_prod_project"]["dataproc_cluster"],
    "region": persist_cfg["gcp_prod_project"]["region"],
    "gcp_conn_id": persist_cfg["gcp_prod_project"]["gcp_conn_id"]
}

for op_name in export_table_dict:
    export_table_dict[op_name]["operator"]=SubDagOperator(
        subdag  = export_to_gcp_dag(
                    "crmdata_bids_bounds."+op_name,
                    sync_data2GCP_dag.schedule_interval,
                    persist_cfg['queue'],
                    sync_data2GCP_dag.default_args,
                    export_table_dict[op_name]["hql_dict"],
                    export_table_params,
                    export_table_dataproc_config
                ),
        task_id = op_name,
        queue   = persist_cfg['queue'],
        dag     = sync_data2GCP_dag
    )
