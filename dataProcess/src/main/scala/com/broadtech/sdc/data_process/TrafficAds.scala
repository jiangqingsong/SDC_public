package com.broadtech.sdc.data_process

import com.broadtech.sdc.common.IpUtils
import com.broadtech.sdc.common.SparkEnvUtil.spark
import java.io.File

import com.broadtech.sdc.common.IpUtils
import com.maxmind.geoip2.DatabaseReader
import org.apache.spark.SparkFiles
import org.apache.spark.sql.functions._

/**
 * @author jiangqingsong
 * @description 流量态势统计分析
 * @date 2020-03-25 9:17
 */
object TrafficAds {
  import spark.implicits._
  /**
   * tab1: 每天访问ip总流量的占比
   * tab2: 每天访问国家的占比
   * tab3： ads_ppe_dpilog_ip_percent
   * @param day 当前天
   * @param minute  当前分钟
   */
  def targetIpTrafficAnalysis(day: String, minute: String, path: String, geoFileName: String): Unit = {
    val trafficIpDf = spark.sql(
      s"""
         |select srcip,dstip,utraffic,dtraffic from $SDC_DETAIL_PRE.$PPE_DPILOG_PRE where day=$day
         |""".stripMargin).as[TrafficIp]
    trafficIpDf.cache()

    val ip2trafficDf = trafficIpDf.withColumn("all_traffic", $"utraffic" + $"dtraffic")
      .drop("utraffic").drop("dtraffic")
      .groupBy("dstip").agg(sum("all_traffic"))
      .toDF("dstip", "all_traffic")
    val allTraffic = ip2trafficDf.select("all_traffic").agg(sum("all_traffic")).head.getDouble(0)

    //tab1:ads_ppe_dpilog_ip_percent
    val ip2trafficPerDf = ip2trafficDf.withColumn("percent", $"all_traffic" / allTraffic)
      .selectExpr("dstip", "all_traffic as traffic", "cast(percent as decimal(20,6)) as percent")
    ip2trafficPerDf.cache()

    val tmpName = "i_tmp"
    ip2trafficPerDf.createOrReplaceTempView(tmpName)
    spark.sql(
      s"""
         |insert overwrite table $SDC_DETAIL.$ADS_PPE_DPILOG_IP_PERCENT partition(day=$day, minute=$minute)
         |select * from $tmpName
         |""".stripMargin)

    ////tab2:ads_ppe_dpilog_country_percent
    spark.sparkContext.addFile(path)
    val countryTrafficDf = ip2trafficPerDf.as[IpPercent].rdd.map(x => {
      val geoFile = SparkFiles.get(geoFileName)
      val reader = new DatabaseReader.Builder(new File(geoFile)).build()
      val country = IpUtils.getCountry(reader, x.dstip)
      (country, x.traffic.toDouble)
    }).reduceByKey( _ + _)
      .map(x =>{

        val precent = x._2 / allTraffic
        CountryPercent(x._1, x._2.toString, precent.toString)
      }).toDF()
    countryTrafficDf.createOrReplaceTempView("c_tmp")
    spark.sql(
      s"""
         |insert overwrite table $SDC_DETAIL.$ADS_PPE_DPILOG_COUNTRY_PERCENT partition(day=$day, minute=$minute)
         |select * from c_tmp
         |""".stripMargin)

    //tab3:ads_ppe_dpilog_ip_percent
    val provinceTrafficDf = trafficIpDf.rdd.map(x => {
      val geoFile = SparkFiles.get(geoFileName)
      val reader = new DatabaseReader.Builder(new File(geoFile)).build()
      val srcProvince = IpUtils.getProvince(reader, x.srcip)
      val rstProvince = IpUtils.getProvince(reader, x.dstip)
      ((srcProvince, rstProvince), x.dtraffic.toDouble + x.utraffic.toDouble)
    }).reduceByKey(_ + _)
      .map(x => ProvincePercent(x._1._1, x._1._2, x._2.toString, (x._2 / allTraffic).toString)).toDF()
    provinceTrafficDf.createOrReplaceTempView("p_tmp")
    spark.sql(
      s"""
         |insert overwrite table $SDC_DETAIL.$ADS_PPE_DPILOG_PROVINCE_PERCENT partition(day=$day, minute=$minute)
         |select * from p_tmp
         |""".stripMargin)
    spark.stop()

  }

  /**
   * 每天port出现次数占比
   * 输入：sdc_detail_pre.ppe_dpilog_pre
   * @param day
   * @param minute
   */
  def portPercentAnalysis(day: String, minute: String): Unit = {

    val trafficPortDf = spark.sql(
      s"""
         |select srcport,dstport from $SDC_DETAIL_PRE.$PPE_DPILOG_PRE where day=$day
         |""".stripMargin).as[TrafficPort]
    trafficPortDf.cache()
    val srcportDf = trafficPortDf.groupBy("srcport").count().toDF("port", "count")
    val dstportDf = trafficPortDf.groupBy("dstport").count().toDF("port", "count")
    val portCountDf = srcportDf.union(dstportDf).groupBy("port").sum("count").toDF("port", "count")
    val totalCount = trafficPortDf.count() * 2

    //精确到小数点后6位
    val portPercentDs = portCountDf.withColumn("percent", col("count") * 1.0 / totalCount)
      .selectExpr("port", "count", "cast(percent as decimal(20,6)) as percent").sort($"count".desc).as[PortPercent]
    /*spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $SDC_DETAIL.$ADS_PPE_DPILOG_PORT_PERCENT(
         |    port string,
         |    count string,
         |    percent string
         |)
         |PARTITIONED BY(day int, minute string)
         |STORED AS ORC
         |""".stripMargin)*/

    val tmpName = "p_tmp"
    portPercentDs.createOrReplaceTempView(tmpName)
    spark.sql(
      s"""
         |insert overwrite table $SDC_DETAIL.$ADS_PPE_DPILOG_PORT_PERCENT partition(day=$day, minute=$minute)
         |select * from $tmpName
         |""".stripMargin)
    spark.stop()
  }
}

case class ProvincePercent(
                           src_province: String,
                           dst_province: String,
                           traffic: String,
                           percent: String
                         )
case class CountryPercent(
                           country: String,
                           traffic: String,
                           percent: String
                      )
case class PortPercent(
                        port: String,
                        count: String,
                        percent: String
                      )

case class IpPercent(
                      dstip: String,
                      traffic: String,
                      percent: String
                    )

case class TrafficPort(
                        srcport: String,
                        dstport: String
                      )

case class TrafficIp(
                      srcip: String,
                      dstip: String,
                      utraffic: String,
                      dtraffic: String
                    )
