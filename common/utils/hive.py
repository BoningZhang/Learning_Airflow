
import logging
from common.utils.gcp_utils import build_dataproc_client, submit_hive_job, wait_dataproc_task, get_result_from_could_storage


def run_hive_query_gcp(hql, gcp_info, is_output=True):
    """
        gcp_info = {
            "region": region,
            "project_id": project_id,
            "dataproc_cluster": dataproc_cluster,
            "gcp_keyfile": gcp_keyfile,
            "result_folder": result_folder
        }
        gcp_info is the config for the gcp dataproc cluster to run hive
    """

    # prepare gcp related parameters


    # build client service for dataproc using gcp service account
    dataproc = build_dataproc_client(gcp_info)

    # submit hive job to dataproc
    job_id = submit_hive_job(dataproc, gcp_info, hql)

    # wait dataproc to succeed
    job_status = wait_dataproc_task(dataproc, gcp_info, job_id)

    if job_status==False:
        logging.error("Dataproc job %s failed in run_hive_query_gcp()..." % job_id)

    # return query results as string from cloud storage blob
    if is_output:
        query_res = get_result_from_could_storage(gcp_info)
        if not query_res:
            raise RuntimeError("Output is empty from the hive query!")

        return query_res
