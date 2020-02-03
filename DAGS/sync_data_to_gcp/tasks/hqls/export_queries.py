export_table1_to_gcp_hql = """
ADD FILE {{ params.gcp_keyfile }};
SET google.cloud.auth.service.account.enable=true;
SET google.cloud.auth.service.account.json.keyfile={{ params.gcp_keyfile }};

CREATE TABLE IF NOT EXISTS table1_{{ params.gen_date_str_nodash(execution_date, params.endDate_delta) }}_gcp_export(
    column1 STRING,
    column2 DOUBLE
) PARTITIONED BY(
    ds  STRING
)
STORED AS PARQUET
LOCATION
  '{{ params.gcp_warehouse_path }}/gcp_database.db/table1_{{ params.gen_date_str_nodash(execution_date, params.endDate_delta) }}'
;

SET parquet.compression=GZIP;

INSERT OVERWRITE TABLE table1_{{ params.gen_date_str_nodash(execution_date, params.endDate_delta) }}_gcp_export
PARTITION (ds = '{{ ds }}')
SELECT column1,
       column2
FROM table1
WHERE ds = '{{ ds }}'
;
"""

add_dataproc_table1_partition_hql = """
USE gcp_database;

CREATE TABLE IF NOT EXISTS table1(
    column1 STRING,
    column2 DOUBLE
) PARTITIONED BY(
    ds STRING
)
STORED AS PARQUET
LOCATION
  '{{ params.gcp_warehouse_path }}/gcp_database.db/table1'
;

LOAD DATA INPATH '{{ params.gcp_warehouse_path }}/gcp_database.db/table1_{{ params.gen_date_str_nodash(execution_date, params.endDate_delta) }}/ds={{ ds }}/*'
OVERWRITE INTO TABLE table1 PARTITION (ds = '{{ ds }}')
;

"""

drop_tmp_table1_hql = """
ADD FILE {{ params.gcp_keyfile }};
SET google.cloud.auth.service.account.enable=true;
SET google.cloud.auth.service.account.json.keyfile={{ params.gcp_keyfile }};

DROP TABLE IF EXISTS table1_{{ params.gen_date_str_nodash(execution_date, params.endDate_delta) }}_gcp_export;
"""
