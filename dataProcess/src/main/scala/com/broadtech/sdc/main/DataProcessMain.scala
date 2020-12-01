package com.broadtech.sdc.main

import com.broadtech.sdc.data_process._
import com.broadtech.sdc.ss.LoadAggrLog

/**
 * 数据处理入口
 */
object DataProcessMain {
  def main(args: Array[String]): Unit = {
    println(args.mkString("|"))
    args(0) match {
      case "dns-process"     => AssetProcess.assetUpdate(args(1), args(2))
    }
  }

}
