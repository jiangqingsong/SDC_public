package com.broadtech.sdc.data_process

import com.broadtech.sdc.common.SparkEnvUtil._
/**
 * @author jiangqingsong
 * @description  离线数据安全设备日志数据
 * @date 2020-03-13 11:40
 */
object TransSecurityLogAndPut {
  def transSecurityLogAndPut(): Unit ={
    val logPath = ""
    val logRdd = spark.sparkContext.textFile(logPath)
  }
}
