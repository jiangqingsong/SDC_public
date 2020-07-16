package com.broadtech.sdc.ss
import java.util.Properties

import com.broadtech.sdc.common.SparkEnvUtil._
import org.apache.spark.sql.SaveMode
/**
 * @author jiangqingsong
 * @description 导入ids日志数据
 * @date 2020-07-16 10:12
 */
object LoadIDSLog {
  import spark.implicits._
  def loadIDSLog(): Unit ={
    val dataPath = "/user/jqs/data/ss/ids.csv"
    val df = spark.read.option("header","true").option("escape", "\"").csv(dataPath)
    val finalDf = df.rdd.map(x => IdsLog(
      x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getString(4), x.getString(5), x.getString(6),
      x.getString(7), x.getString(8), x.getString(9), x.getString(10), x.getString(11), x.getString(12), x.getString(13),
      x.getString(14), x.getString(15), x.getString(16), x.getString(17), x.getString(18), x.getString(19), x.getString(20),
      x.getString(21), x.getString(22), x.getString(23), x.getString(24), x.getString(25), x.getString(26), x.getString(27),
      x.getString(28), x.getString(29), x.getString(30), x.getString(31), x.getString(32), x.getString(33), x.getString(34),
      x.getString(35), x.getString(36), x.getString(37), x.getString(38), x.getString(39), x.getString(40), x.getString(41),
      x.getString(42), x.getString(43), x.getString(44), x.getString(45), x.getString(46), x.getString(47), x.getString(48),
      x.getString(49), x.getString(50), x.getString(51), x.getString(52), x.getString(53), x.getString(54), x.getString(55),
      x.getString(56), x.getString(57), x.getString(58), x.getString(59), x.getString(60), x.getString(61), x.getString(62),
      x.getString(63), x.getString(64), x.getString(65), x.getString(66), x.getString(67), x.getString(68), x.getString(69),
      x.getString(70)
    )).toDF()
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("url", "jdbc:mysql://192.168.5.93:3306/sdc?characterEncoding=utf8")
    finalDf.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), "ss_ids_log", prop)
    spark.stop()
  }

  case  class IdsLog(
                      index1: String,
                      type1: String,
                      id: String,
                      score: String,
                      source_lid: String,
                      source_imergecount: String,
                      source_ceventname: String,
                      source_ceventdigest: String,
                      source_ceventtype: String,
                      source_icollecttype: String,
                      source_ieventlevel: String,
                      source_iprotocol: String,
                      source_iappprotocol: String,
                      source_csrcname: String,
                      source_csrcmac: String,
                      source_csrcip: String,
                      source_csrctip: String,
                      source_isrcport: String,
                      source_isrctport: String,
                      source_cdstname: String,
                      source_cdstmac: String,
                      source_cdstip: String,
                      source_cdsttip: String,
                      source_idstport: String,
                      source_idsttport: String,
                      source_cusername: String,
                      source_cprogram: String,
                      source_coperation: String,
                      source_cobject: String,
                      source_iresult: String,
                      source_ireponse: String,
                      source_cdevname: String,
                      source_cdevtype: String,
                      source_cdevip: String,
                      source_loccurtime: String,
                      source_lrecepttime: String,
                      source_ccollectorip: String,
                      source_lsend: String,
                      source_lreceive: String,
                      source_lduration: String,
                      source_dmonitorvalue: String,
                      source_corilevel: String,
                      source_coritype: String,
                      source_crequestmsg: String,
                      source_ceventmsg: String,
                      source_eventfields_istandby2: String,
                      source_eventfields_istandby3: String,
                      source_eventfields_istandby1: String,
                      source_eventfields_cstandby11: String,
                      source_eventfields_cstandby12: String,
                      source_eventfields_cstandby10: String,
                      source_eventfields_lstandby4: String,
                      source_eventfields_lstandby3: String,
                      source_eventfields_lstandby2: String,
                      source_eventfields_lstandby1: String,
                      source_eventfields_lstandby6: String,
                      source_eventfields_lstandby5: String,
                      source_eventfields_istandby6: String,
                      source_eventfields_istandby4: String,
                      source_eventfields_istandby5: String,
                      source_eventfields_dstandby1: String,
                      source_eventfields_dstandby2: String,
                      source_eventfields_cstandby1: String,
                      source_eventfields_cstandby6: String,
                      source_eventfields_cstandby7: String,
                      source_eventfields_cstandby8: String,
                      source_eventfields_cstandby9: String,
                      source_eventfields_cstandby2: String,
                      source_eventfields_cstandby3: String,
                      source_eventfields_cstandby4: String,
                      source_eventfields_cstandby5: String
                    )
}
