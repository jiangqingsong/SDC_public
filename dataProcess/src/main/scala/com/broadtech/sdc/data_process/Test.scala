package com.broadtech.sdc.data_process

import com.broadtech.sdc.common.SparkEnvUtil._
/**
 * @author jiangqingsong
 * @description
 * @date 2020-04-20 16:22
 */
object Test {
  def main(args: Array[String]): Unit = {
    val df = spark.sql("select * from vulnerability limit 5")
    println(df.printSchema())
  }
}
