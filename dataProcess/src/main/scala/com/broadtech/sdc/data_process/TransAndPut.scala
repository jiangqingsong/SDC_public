package com.broadtech.sdc.data_process

import com.broadtech.sdc.common.SparkEnvUtil._

/**
 * 离线解析入库:【仅供测试阶段使用，目前环境不用这一套】
 * 1、dpi采集数据
 * 2、防火墙采集数据
 */
object TransAndPut {
  def transAndPut(tabName: String, date: String, hour: String, separator: String): Unit = {
    tabName match {
      case "dpi" => transDpiAndPut(date, separator)
      case "firewall" => transFirewallAndPut(date, hour, separator)
    }
  }

  /**
   * 直接读取hdfs上文件，做解析入库
   * @param date
   * @param separator
   */
  def transDpiAndPut(date: String, separator: String): Unit = {
    import spark.implicits._
    val filePath = "/xxx/dpi" + date + "/*"
    val sourceRdd = spark.sparkContext.wholeTextFiles(filePath)
    val dpiDs = sourceRdd.map(_._2).map(line => {
      val cols = line.split(separator)
      Dpi(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6), cols(7), cols(8), cols(9), cols(10), cols(11),
        cols(12), cols(13), cols(14), cols(15), cols(16), cols(17), cols(18), cols(19), cols(20), cols(21), cols(22))
    }).toDF
    val distinctedDpiDf = dpiDs.distinct()
    val tmpName = "tmp_tab"
    distinctedDpiDf.createGlobalTempView(tmpName)
    spark.sql(
      s"""
         |insert into ods_dpi partition(date=$date) select * from $tmpName
         |""".stripMargin)

    spark.stop()
  }

  /**
   * 直接读取hdfs上文件，做解析入库
   * @param date
   * @param hour
   * @param separator
   */
  def transFirewallAndPut(date: String, hour: String, separator: String): Unit = {
    import spark.implicits._

    //val filePath = "/xxx/firewall/" + date + "/*"
    val filePath = "/tmp/fireWallData/20-03-05/" + hour
    /**
     * 区别wholeTextFiles和textFile（参考源码）
     */
    //val sourceRdd = spark.sparkContext.wholeTextFiles(filePath)
    val sourceRdd = spark.sparkContext.textFile(filePath)

    val firewallDf = sourceRdd.map(line => {
      line.split("filterlog:")
    }).filter(_.length == 2)
      .map(x => {
        val timeSplit = x(0).split(" ")
        (timeSplit.slice(0, timeSplit.length-1).mkString(" "), x(1))
      })
      .map(x => (x._1, x._2.split(",")))
      .map(y => {
        //原始数据字段个数不固定，需要做判断处理
        val cols = y._2
        val interf = cols(4)
        val timestamp = y._1
        val action = cols(6)
        val dstip = if(cols.length > 19) cols(19) else ""
        val dstport = if(cols.length > 21) cols(21) else ""
        val id = cols(12)
        val ipflags = cols(14)
        val length = if(cols.length > 17) cols(17) else ""
        val protoname = cols(16)
        val reason = cols(5)
        val seq = if(cols.length > 24) cols(24) else ""
        val srcip = if(cols.length > 19) cols(19) else ""
        val srcport = if(cols.length > 20) cols(20) else ""
        val tcpflags = if(cols.length > 23) cols(23) else ""
        Firewall(
          interf,timestamp,action,dstip,dstport,id,ipflags,length,protoname,reason,seq,srcip,srcport,tcpflags
        )
      }).toDF()
    val distinctedFirewallDf = firewallDf.distinct()
    val tmpName = "tmp_tab"
    distinctedFirewallDf.createOrReplaceTempView(tmpName)
    //todo  如果按小时分区就需要加小时分区
    spark.sql(
      s"""
         |insert into sdc.ods_firewall_online partition(pt_d=$date, pt_h=$hour) select * from $tmpName
         |""".stripMargin)

    spark.stop()
  }
}

case class Dpi(
                srcIPType: String,
                dstIPType: String,
                l4proto: String,
                assetType: String,
                protocol: String,
                srcPort: String,
                dstPort: String,
                startTime: String,
                endTime: String,
                ulTraffic: String,
                dlTraffic: String,
                ulPacktes: String,
                dlPackets: String,
                srcIP: String,
                dstIP: String,
                devType: String,
                devName: String,
                softName: String,
                softVer: String,
                vendor: String,
                os: String,
                osVer: String,
                netType: String
              )

case class Firewall(
                     interf: String,
                     timestamp: String,
                     action: String,
                     dstip: String,
                     dstport: String,
                     id: String,
                     ipflags: String,
                     length: String,
                     protoname: String,
                     reason: String,
                     seq: String,
                     srcip: String,
                     srcport: String,
                     tcpflags: String
                   )