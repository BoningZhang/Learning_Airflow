from airflow.models import DAG
from airflow.operators import HiveOperator, BashOperator
from datetime import timedelta

from common.operators.dataproc_operator import DataProcHiveOperator

def export_to_gcp_dag(_sub_dag_id, _schedule_interval, _queue, _default_args, _export_hql_dict, _params, _dataproc_config):
    dag = DAG(dag_id = _sub_dag_id,
              schedule_interval = _schedule_interval,
              default_args = _default_args
          )

    export_data_hql = _export_hql_dict["export_data_hql"]
    add_dataproc_partition_hql = _export_hql_dict["add_dataproc_partition_hql"]
    drop_tmp_table_hql = _export_hql_dict["drop_tmp_table_hql"]

    def gen_date_str_nodash(execution_date, days_delta=0, hours_delta = 0):
        from pytz import timezone, utc
        from datetime import datetime, timedelta

        pacific_timezone = "US/Pacific"
        date = utc.localize(execution_date)
        date = date.astimezone(timezone(pacific_timezone))

        if days_delta:
            date += timedelta(days=days_delta)

        if hours_delta:
            date += timedelta(hours=hours_delta)

        return date.strftime("%Y%m%d")

    _params.update({"gen_date_str_nodash": gen_date_str_nodash})

    export_data = HiveOperator(
            task_id = "export_data",
            hql     = export_data_hql,
            params  = _params,
            queue   = _queue,
            dag     = dag
        )

    add_dataproc_partition = DataProcHiveOperator(
            task_id          = "add_dataproc_partition",
            dataproc_cluster = _dataproc_config["dataproc_cluster"],
            region           = _dataproc_config["region"],
            gcp_conn_id      = _dataproc_config["gcp_conn_id"],
            query            = add_dataproc_partition_hql,
            params  = _params,
            queue   = _queue,
            dag     = dag
        )

    drop_tmp_table = HiveOperator(
            task_id = "drop_tmp_table",
            hql     = drop_tmp_table_hql,
            params  = _params,
            queue   = _queue,
            dag     = dag
        )

    add_dataproc_partition.set_upstream(export_data)
    drop_tmp_table.set_upstream(add_dataproc_partition)

    if _params.get("stamp_file_path", None) is not None:
        gcp_conf_true = 'google.cloud.auth.service.account.enable=true'
        gcp_conf_keyfile = 'google.cloud.auth.service.account.json.keyfile={{ params.gcp_keyfile }}'

        add_success_stamp_file = BashOperator(
            task_id="add_success_stamp_file",
            bash_command="hadoop fs -D " + gcp_conf_true + " -D " + gcp_conf_keyfile + " -touchz " + _params.get("stamp_file_path", None),
            params=_params,
            queue=_queue,
            dag=dag
        )
        add_success_stamp_file.set_upstream(drop_tmp_table)

    return dag
