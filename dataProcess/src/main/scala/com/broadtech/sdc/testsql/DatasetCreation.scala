package com.broadtech.sdc.testsql

import org.apache.spark.sql.SparkSession

/**
 * @author jiangqingsong
 * @description
 * @date 2020-07-17 18:06
 */
object DatasetCreation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._

    val originRdd = sc.makeRDD(Array("s.pcmgr.qq.com|116.128.163.62;140.206.164.254|20200915000525.026|20200915000525.026|1"))

    val sep = "\\|"
    originRdd.map(_.split(sep)).map(arr => {
      val hostName = arr(0)
      val ipList = arr(1)
      val timestamp = arr(2)
      val count = arr(4)
      val ips = ipList.split(";")
      ips.map(ip => {
        hostName + "|" + ip + "|" + timestamp + "|" + count
      })
    }).flatMap(x => x.map(y => y)).foreach(println)
  }
}
