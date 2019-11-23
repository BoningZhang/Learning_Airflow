import subprocess
import logging

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import apply_defaults
from airflow.macros import ds_add

from common.utils.gcp_utils import check_stamp_files_gcp


class HiveTableStampSensorGCP(BaseSensorOperator):
    """
    Waits for a list of gcp stamp files

    :param service_account: The gcp json key file for the service account
        with permission to access the stamp files
    :type service_account: string

    """
    @apply_defaults
    def __init__(self,
                 service_account,
                 stamp_file_template = None,
                 stamp_file_cb = None,
                 days_delta = 0,
                 *args,
                 **kwargs):
        super(HiveTableStampSensorGCP, self).__init__(*args, **kwargs)
        self.service_account = service_account
        self.stamp_file_template = stamp_file_template
        self.stamp_file_cb = stamp_file_cb
        self.days_delta = days_delta

    def poke(self, context):
        if self.stamp_file_cb is None:
            stamp_file = [self.stamp_file_template%(ds_add(context["ds"], self.days_delta))]
        else:
            stamp_file = self.stamp_file_cb(self.stamp_file_template, context)

        stamp_files = []
        if isinstance(stamp_file, basestring):
            stamp_files = [stamp_file]
        else:
            stamp_files = stamp_file
        # check each file in path array: stamp_files
        ret_code = check_stamp_files_gcp(self.service_account, stamp_files)
        return ret_code


#
