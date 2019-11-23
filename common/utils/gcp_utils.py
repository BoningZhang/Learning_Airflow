import re
import logging
import uuid
import time
from google.cloud import storage
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from app_config.sem import retrieve_persist_cfg


def extract_gcs_path(path):
    # we will always assume the object path should be in
    # gs://bucket/folder/.../file
    # format

    m = re.match(r"^gs://(.*?)/(.+)", path)

    if m:
        return m.group(1), m.group(2)

    raise Exception("failed to path gcs path %s to extract bucket and path" % path)


def check_stamp_files_gcp(service_account, stamp_files):
    # google-cloud-storage==1.16.1
    client = storage.Client.from_service_account_json(service_account)

    for stamp_file in stamp_files:
        bucket_name, prefix_name = extract_gcs_path(stamp_file)
        logging.info("bucket_name: %s, prefix_name: %s" % (bucket_name, prefix_name))
        bucket = client.get_bucket(bucket_name)
        blobs_array = map(lambda x: x, bucket.list_blobs(prefix=prefix_name))
        if len(blobs_array)==0:
            logging.info(stamp_file + " doesn't exist...")
            return False
        else:
            logging.info(stamp_file + " does exist...")

    return True

# the following is for run_hive_query_gcp

def build_dataproc_client(gcp_info):
    # apiclient==1.0.3
    # https://developers.google.com/analytics/devguides/config/mgmt/v3/quickstart/service-py

    gcp_keyfile = gcp_info["gcp_keyfile"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(gcp_keyfile)
    api_name='dataproc'
    api_version='v1'
    dataproc = build(api_name, api_version, credentials=credentials)

    return dataproc

def submit_hive_job(dataproc, gcp_info, hql):
    region = gcp_info["region"]
    project_id = gcp_info["project_id"]
    dataproc_cluster = gcp_info["dataproc_cluster"]
    result_folder = gcp_info["result_folder"]

    # store the query results into cloud storage
    final_hql = """
        INSERT OVERWRITE DIRECTORY '{result_folder_}'
        ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
        LINES TERMINATED BY "\n"
        {hql_}
    """.format(result_folder_=result_folder, hql_=hql)

    job_details = {
        "projectId" : project_id,
        "job" : {
            "placement": {
                "clusterName": dataproc_cluster,
            },
            "submittedBy": "run_hive_query_gcp_airflow",
            "hiveJob": {
                "queryList": {
                    "queries": [final_hql]
                }
            }
        }
    }

    result = dataproc.projects().regions().jobs().submit(
                projectId=project_id,
                region=region,
                body=job_details
            ).execute()

    dataproc_job_id = result['reference']['jobId']
    logging.info("Submit hive job %s in dataproc running query: \n %s" % (dataproc_job_id, final_hql) )
    return dataproc_job_id


def wait_dataproc_task(dataproc, gcp_info, job_id):

    # consts parameter for dataproc job status
    DATAPROC_POLLING_INTERVAL_SECONDS = 15
    STATE_ERROR   = "ERROR"
    STATE_DONE    = "DONE"
    STATE_RUNNING = "RUNNING"

    region = gcp_info["region"]
    project_id = gcp_info["project_id"]

    logging.info("waiting for job %s to finish..." % job_id)

    while True:
        time.sleep(DATAPROC_POLLING_INTERVAL_SECONDS)
        result = dataproc.projects().regions().jobs().get(
            projectId = project_id,
            region    = region,
            jobId     = job_id
        ).execute()
        status = result["status"]["state"]

        if status == STATE_ERROR:
            logging.error("failed with the job id %s result %s" % (job_id, result))
            return False

        elif status == STATE_DONE:
            logging.info("finished with the job id %s result %s" % (job_id, result))
            return True

        elif status == STATE_RUNNING:
            # uncommnet it when the yarn status is up to date
            #progress = result["yarnApplications"][0]["progress"] * 100
            #logging.info("the jobs id %s is still running progress %d%%..." % (job_id, progress))
            logging.info("the spark job id %s is still running..." % job_id)

        else:
            logging.error("unkown state for job id %s result %s" % (job_id, result))
            return False

    return False

def list_blobs(gcp_info):
    result_folder = gcp_info["result_folder"]
    gcp_keyfile = gcp_info["gcp_keyfile"]

    bucket_name, prefix_name = extract_gcs_path(result_folder)
    client = storage.Client.from_service_account_json(gcp_keyfile)
    bucket = client.get_bucket(bucket_name)

    blobs_array = map(lambda x: x, bucket.list_blobs(prefix=prefix_name))

    return blobs_array

def get_result_from_could_storage(gcp_info):
    blobs_array = list_blobs(gcp_info)

    res_string = ""
    for blob in blobs_array:
        res_string += "\n" + blob.download_as_string()

    return res_string


#
