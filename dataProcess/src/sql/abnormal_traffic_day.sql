CREATE TABLE IF NOT EXISTS sdc_detail.ads_abnormal_traffic_day (
 date string,
 total_traffic string,
 normal_traffic string,
 abnormal_traffic string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS textfile
;