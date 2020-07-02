package com.broadtech.sdc.data_process

import com.broadtech.sdc.common.SparkEnvUtil._
/**
 * 1、读取mysql数据df1
 * 2、读取hive分区数据df2
 * 3、计算df1的两种assetId
 * 4、分别与df2做map和union操作
 * 5、覆盖hive分区数据
 */
object AssetProcess {
  def assetUpdate(date: String, hour: String): Unit ={
    val url = ""
    val dbtable = ""
    val user = ""
    val password = ""
    val hiveTabName = ""
    val assetId = "asset_id"
    import spark.implicits._
    val mysqlDf = spark.read.format("jdbc")
      .option("url", s"${url}?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
      .option("dbtable", dbtable)
      .option("user", user)
      .option("password", password)
      .load()

    /**
     * 一\ rec sys:
     * 1、
     */
    val hiveDf = spark.sql(
      s"""
         |select * from $hiveTabName where pt_d = $date and pt_h = $hour
         |""".stripMargin)

    mysqlDf.cache()
    hiveDf.cache()

    val df1_id = mysqlDf.select(assetId).rdd.map(x => x.getString(0))
    //val df1_id_map_bc = spark.sparkContext.broadcast(df1_id_map)

    val df2_id = hiveDf.select(assetId).rdd.map(_.getString(0))
    val updateIdRdd = df1_id.intersection(df2_id)
    val insertIdRdd = df1_id.subtract(df2_id)
    val updateIdMap = spark.sparkContext.broadcast(updateIdRdd.map((_, 1)).collect())
    //updateIdRdd用于hive分区数据map操作修改对应row的数据
    //insertIdRdd用于直接和hive分区数据union
    val updatedHiveDf = hiveDf.filter(row => !updateIdMap.value.contains(row.getAs[String](assetId)))
      .union(mysqlDf)
    updatedHiveDf.createGlobalTempView("updateHiveTab")
    //overwrite partition
    spark.sql(
      s"""
        |insert overwrite $hiveTabName where pt_d=$date and pt_h=$hour
        |select * from updateHiveTab
        |""".stripMargin)
    spark.stop()
  }
}
