package com.broadtech.sdc.common

import org.apache.spark.sql.SparkSession

/**
 * spark env
 */
object SparkEnvUtil {
  val spark = SparkSession
    .builder
    .appName("SDC sparkPlatform")
    .enableHiveSupport
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate
}
