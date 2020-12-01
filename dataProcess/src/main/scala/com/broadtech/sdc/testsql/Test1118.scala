package com.broadtech.sdc.testsql

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.spark.sql.SparkSession

/**
 * @author jiangqingsong
 * @description
 * @date 2020-11-18 15:03
 */
object Test1118 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val data = sc.makeRDD(Array(Person1("test", "11")))

    data.map(x => {
      JSON.toJSONString(x, SerializerFeature.EMPTY: _*)
    }).foreach(println)
  }
}

case class Person1(name: String, age: String)