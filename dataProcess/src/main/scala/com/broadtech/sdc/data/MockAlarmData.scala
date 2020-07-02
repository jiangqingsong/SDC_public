package com.broadtech.sdc.data

import scala.util.Random

/**
 * @author jiangqingsong
 * @description mock 告警数据
 * @date 2020-04-02 14:05
 */
object MockAlarmData {
  val rand = new Random()
  def main(args: Array[String]): Unit = {
    for(i <- 0 until(100)){
      println(mockData)
    }

  }

  def mockData(): String ={
    val event_title = Seq("挖矿蠕虫WannaMine连接DNS服务器通信", "挖矿程序连接矿池服务器通信", "[50450]SNMP操作使用弱口令",
      "Firefox Hyphenated URL Exploit")(getRandom(4))
    val event_type = ""
    val event_level = Seq("中","高","低")(3)
    val event_time = "2019-09-27 13:" + getRandom(60) + ":" + getRandom(60)
    val alert_count = getRandom(100)
    val event_dev_ip = Seq("192.168.5.104", "192.168.5.112", "192.168.5.113", "192.168.5.114", "192.168.5.115")(getRandom(5))
    val event_dev_type = ""
    val event_source_ip = Seq("192.168.5.104", "192.168.5.112", "192.168.5.113", "192.168.5.114", "192.168.5.115")(getRandom(5))
    val event_source_port = Seq("54871", "50448", "57920", "56310", "63261", "4462", "53", "52751", "52752")(getRandom(9))
    val event_target_ip = Seq("192.168.5.104", "192.168.5.112", "192.168.5.113", "192.168.5.114", "192.168.5.115")(getRandom(5))
    val event_target_port = Seq("54871", "50448", "57920", "56310", "63261", "4462", "53", "52751", "52752")(getRandom(9))
    val event_affected_dev = ""
    val event_description = ""
    val source_iphplace = "四川省成都市"
    val dest_iphplace = "四川省成都市"
    Seq(event_title, event_type, event_level, event_time, alert_count, event_dev_ip, event_dev_type, event_source_ip, event_source_port, event_target_ip,
      event_target_port, event_affected_dev, event_description, source_iphplace, dest_iphplace).mkString("|")
  }
  def getRandom(threshold: Int): Int ={
    rand.nextInt(threshold)
  }
}
