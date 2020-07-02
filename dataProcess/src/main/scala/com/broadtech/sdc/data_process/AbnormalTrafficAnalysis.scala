package com.broadtech.sdc.data_process

import com.broadtech.sdc.common.DateUtil.getPreMulHoursTime
import com.broadtech.sdc.common.SparkEnvUtil._
import com.broadtech.sdc.case_class.traffic.AbnormalTrafficDay

/**
 * @author jiangqingsong
 * @description 异常流量分析
 * @date 2020-03-22 10:58
 */
object AbnormalTrafficAnalysis {

  import spark.implicits._

  private val ABNORMAL_MINUTE_TAB = "sdc_detail.ppe_dpilog_abnormal"
  private val ABNORMAL_MINUTE_DAY = "sdc_detail.ads_abnormal_traffic_day"
  private val ABNORMAL_MINUTE_WEEK = "sdc_detail.ads_abnormal_traffic_week"
  private val ABNORMAL_MINUTE_MONTH = "sdc_detail.ads_abnormal_traffic_month"

  /**
   *
   * @param strategy 根据不同的策略统计
   * @param day
   */
  def abnormalTrafficAnalysisMain(strategy: String, day: String): Unit = {
    val tabName = "sdc_detail.ppe_dpilog_abnormal"
    strategy match {
      case "day"   => abnormalTrafficAnalysisForWeek(day, 1, tabName, ABNORMAL_MINUTE_DAY)
      case "week"  => abnormalTrafficAnalysisForWeek(day, 7, tabName, ABNORMAL_MINUTE_WEEK)
      case "month" => abnormalTrafficAnalysisForWeek(day, 30, tabName, ABNORMAL_MINUTE_MONTH)
    }
  }
  /**
   * 离线计算时间范围内的异常流量数据
   *
   * @param day 当前日期
   * @param srcTabName
   */
  def abnormalTrafficAnalysisForWeek(day: String, windowSize: Int, srcTabName: String, targetTabName: String): Unit = {
    val preDay = getPreMulHoursTime(-24 * windowSize, "yyyyMMdd", day)
    val startTime = preDay + "0000"
    val endTime = day + "0000"
    val sampleDf = spark.sql(
      s"""
         |select * from $srcTabName where day>=$startTime and day<$endTime
         |""".stripMargin).as[AbnormalTraffic]
    val abnormalTrafficDayDf = sampleDf.rdd.map(a => {
      (1, (a.total_traffic.toDouble, a.normal_traffic.toDouble, a.abnormal_traffic.toDouble))
    })
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
      .map(c => {
        windowSize match {
          case 1 => AbnormalTrafficDay(preDay, c._2._1.toString, c._2._2.toString, c._2._3.toString)
          case 7 => AbnormalTrafficDay(startTime.substring(0,8) + "_" + endTime.substring(0,8), c._2._1.toString, c._2._2.toString, c._2._3.toString)
          case 30 => AbnormalTrafficDay(preDay.substring(0,6), c._2._1.toString, c._2._2.toString, c._2._3.toString)
        }
      })
      .toDF()

    val viewName = "tmp_abnormal"
    abnormalTrafficDayDf.createOrReplaceTempView(viewName)
    spark.sql(
      s"""
         |insert into table $targetTabName
         |select * from $viewName
         |""".stripMargin)
    spark.stop()
  }

  /**
   * 计算该小时内的异常流量，结果以分钟粒度保存
   *
   * @param day       当前日期
   * @param minute    当前小时 1300表示13点
   * @param tabName
   * @param windowDay 样本时间窗口大小
   */
  def abnormalTrafficAnalysisBase(day: String, minute: String, tabName: String, windowDay: String = "7") {

    val startTime = getPreMulHoursTime(-24 * windowDay.toInt, "yyyyMMdd", day)
    val sampleDf = spark.sql(
      s"""
         |select starttime,utraffic,dtraffic from $tabName where day<=$day and day>$startTime
         |""".stripMargin).as[DpiFeature]

    val currentDf = spark.sql(
      s"""
         |select starttime,utraffic,dtraffic from $tabName where day=$day and minute=$minute
         |""".stripMargin).as[DpiFeature]
    currentDf.cache()

    //计算当前小时的异常流量阈值(hour, (dBound, uBound)) -- hour （下限，上限）
    val abnormalThresholdMap = sampleDf.rdd.filter(!_.starttime.isEmpty).map(l => {
      (l.starttime.substring(11, 13), l.utraffic.toInt + l.dtraffic.toInt)
    }).groupByKey().map(x => {
      val hour = x._1
      val size = x._2.size
      val avgTraffic = x._2.sum.toDouble / size
      val sumStandardTraffic = if (size == 0) {
        0.0
      } else {
        x._2.map(y => (y - avgTraffic) * (y - avgTraffic)).reduce(_ + _)
      }
      val standartTraffic = math.sqrt(sumStandardTraffic / size) //标准差
      val uBound = avgTraffic + standartTraffic
      val dBound = avgTraffic - standartTraffic
      (hour, (dBound, uBound))
    }).collect().toMap
    //下面就是

    val abnormalThresholdBC = spark.sparkContext.broadcast(abnormalThresholdMap)
    val sumTraffic = currentDf.rdd.map(x => x.dtraffic.toInt + x.utraffic.toInt).reduce(_ + _)
    val avgCurrentTraffic = sumTraffic.toDouble / currentDf.count()
    val avgCurrentTrafficBC = spark.sparkContext.broadcast(avgCurrentTraffic)


    val abnormalTrafficDf = currentDf.rdd.filter(!_.starttime.isEmpty).map(x => {
      val abnormalThresholdMap = abnormalThresholdBC.value
      val year = x.starttime.substring(0, 4)
      val month = x.starttime.substring(5, 7)
      val day = x.starttime.substring(8, 10)
      val hour = x.starttime.substring(11, 13)
      val minute = x.starttime.substring(14, 16)
      val key = year + month + day + hour + minute
      val thresholdTuple = abnormalThresholdMap.get(hour).getOrElse((0.0, avgCurrentTrafficBC.value))
      val uBound = thresholdTuple._2
      val dBound = /*thresholdTuple._1*/ 0.0 //todo 这里放出来导致很多异常流量产生，模型需要优化
      val totalTraffic = x.utraffic.toDouble + x.dtraffic.toDouble
      val abnormalTraffic = if (totalTraffic > uBound) {
        totalTraffic - uBound
      } else if (totalTraffic < dBound) {
        totalTraffic
      } else {
        0.0
      }
      val normalTraffic = totalTraffic - abnormalTraffic
      (key, (totalTraffic, normalTraffic, abnormalTraffic, dBound, uBound))
    })
      .reduceByKey((a, b) => {
        (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4, a._5) //同一小时内上下界相同
      }).map(x => {
      val totalTraffic = x._2._1
      val normalTraffic = x._2._2.toString
      val abnormalTraffic = x._2._3
      val dBound = x._2._4.toString
      val uBound = x._2._5.toString
      //eg: 202004201627,20000,5000,15000,xxx,xxx
      AbnormalTraffic(x._1, totalTraffic.toString, normalTraffic.toString, abnormalTraffic.toString, dBound, uBound)
    }).toDF()

    val viewName = "ab_tmp"
    abnormalTrafficDf.createOrReplaceTempView(viewName)
    //insert到一个分钟粒度表
    spark.sql(
      s"""
         |insert overwrite table sdc_detail.ppe_dpilog_abnormal partition(day=$day,minute=$minute)
         |select * from $viewName
         |""".stripMargin)
    spark.stop()
  }
}

case class AbnormalTraffic(
                            starttime: String,
                            total_traffic: String,
                            normal_traffic: String,
                            abnormal_traffic: String,
                            d_bound: String,
                            u_bound: String
                          )

case class DpiFeature(
                       starttime: String,
                       utraffic: String,
                       dtraffic: String
                     )
