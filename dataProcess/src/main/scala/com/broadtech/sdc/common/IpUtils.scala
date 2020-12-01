package com.broadtech.sdc.common

import java.net.InetAddress
import com.maxmind.geoip2.DatabaseReader
/**
 * @author leo.J
 * @description  IP 转换工具类
 * @date 2020-03-25 16:23
 */
object IpUtils {
  /**
   * 获得城市
   * @param reader
   * @param ip
   * @return
   */
  def getCity(reader: DatabaseReader, ip: String): String ={
    reader.city(InetAddress.getByName(ip)).getCity().getNames().get("zh-CN")
  }

  /**
   * 获取省份
   * @param reader
   * @param ip
   * @return
   */
  def getProvince(reader: DatabaseReader, ip: String): String ={
    reader.city(InetAddress.getByName(ip)).getMostSpecificSubdivision().getNames().get("zh-CN")
  }

  /**
   * 获取国家
   * @param reader
   * @param ip
   * @return
   */
  def getCountry(reader: DatabaseReader, ip: String): String ={
    reader.city(InetAddress.getByName(ip)).getCountry().getNames().get("zh-CN")
  }

}
