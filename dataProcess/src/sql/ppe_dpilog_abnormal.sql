CREATE TABLE IF NOT EXISTS sdc_detail.ppe_dpilog_abnormal (
 starttime string,
 total_traffic string,
 normal_traffic string,
 abnormal_traffic string,
 d_bound string,
 u_bound string
)
PARTITIONED BY(day int, minute string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS ORC
;