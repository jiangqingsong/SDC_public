package com.broadtech.sdc.common

import java.text.{ParsePosition, SimpleDateFormat}
import java.util.{Calendar, Date}

object DateUtil {
  def getGapMinutes(pattern: String, t1: String, t2: String): Double ={
    val fm = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    math.abs((fm.parse(t1).getTime - fm.parse(t2).getTime)/1000.0/60)
  }

  def getGapSeconds(pattern: String, t1: String, t2: String): Double ={
    val fm = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    math.abs((fm.parse(t1).getTime - fm.parse(t2).getTime)/1000.0)
  }

  def addMulMinutes(t1: String, n: Int, pattern: String): String ={
    val fm = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    new SimpleDateFormat(pattern).format(fm.parse(t1).getTime + n * 60 * 1000)
  }
  /**
   * 获取n小时差距的时间
   * @param n
   * @param pattern
   * @param time
   * @return
   */
  def getPreMulHoursTime(n: Int, pattern: String, time: String): String = {
    val newTimestamp = (new SimpleDateFormat(pattern)).parse(time, new ParsePosition(0)).getTime() + n*3600*1000
    new SimpleDateFormat(pattern).format(new Date(newTimestamp))
  }

  /**
   * timestamp convert to formatted time.
   *
   * @param timestamp
   * @param pattern
   * @return
   */
  def convertTimestamp2Date(timestamp: String, pattern: String): String = {
    val format = new SimpleDateFormat(pattern)
    format.format(new Date(timestamp.toLong))
  }

  def strDateFormat(strDate: String, inputFormat: String, outputFormat: String): String = {
    val input = new SimpleDateFormat(inputFormat)
    val output = new SimpleDateFormat(outputFormat)
    val date = input.parse(strDate)
    val strFormatedDate = output.format(date)
    return strFormatedDate
  }

  def dateAdd(strDate: String, addType: Int, addNum: Int, format: String): String = {
    val dateFormat = new SimpleDateFormat(format)
    val date = dateFormat.parse(strDate)
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(addType, addNum)
    val strFormatedDate = dateFormat.format(calendar.getTime())
    return strFormatedDate
  }


  def dateAdd(date: Date, addType: Int, addNum: Int, format: String): String = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(addType, addNum)
    val dateFormat = new SimpleDateFormat(format)
    val strFormatedDate = dateFormat.format(calendar.getTime())
    return strFormatedDate
  }

  def dateAdd(date: Date, addType: Int, addNum: Int): Date = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(addType, addNum)
    val dateTime = calendar.getTime()
    return dateTime
  }

  def toDate(strDate: String, format: String): Date = {
    val dateFormat = new SimpleDateFormat(format)
    val date = dateFormat.parse(strDate)
    return date
  }

  def toDateString(date: Date, format: String): String = {
    val dateFormat = new SimpleDateFormat(format)
    val strDate = dateFormat.format(date)
    return strDate
  }

  def toDateString(longDate: Long, format: String): String = {
    val dateFormat = new SimpleDateFormat(format)
    val strDate = dateFormat.format(longDate * 1000)
    return strDate
  }
}
