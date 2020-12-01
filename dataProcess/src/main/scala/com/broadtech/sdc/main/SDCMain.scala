package com.broadtech.sdc.main

import com.broadtech.sdc.data_process.{AbnormalTrafficAnalysis, AggrWithVulnerability, AssetProcess, TrafficAds, TransAndPut}
import com.broadtech.sdc.ss.LoadAggrLog

/**
 * SDC 离线任务入口类
 */
object SDCMain {
  def main(args: Array[String]): Unit = {
    println(args.mkString("|"))
    args(0) match {
      case "assetUpdate"        => AssetProcess.assetUpdate(args(1), args(2))
        //离线解析入库
      case "transAndPut"        => TransAndPut.transAndPut(args(1), args(2), args(3), args(4))
        //dpi和漏洞库关联
      case "dpi_aggr"           => AggrWithVulnerability.aggrDpiWithVulnerability(args(1), args(2), args(3), args(4))
        //主动探测和漏洞库关联
      case "scan_aggr"          => AggrWithVulnerability.aggrScanWithVulnerability(args(1), args(2), args(3), args(4))
        //生成异常流量表
      case "abnormal_traffic"   => AbnormalTrafficAnalysis.abnormalTrafficAnalysisBase(args(1), args(2), args(3), args(4))
        //生成天周月异常流量表
      case "abnormal_traffic_diff"   => AbnormalTrafficAnalysis.abnormalTrafficAnalysisMain(args(1), args(2))
        //端口分布统计
      case "port_percent"       => TrafficAds.portPercentAnalysis(args(1), args(2))
        //ip分布统计
      case "ip_percent"         => TrafficAds.targetIpTrafficAnalysis(args(1), args(2), args(3), args(4))
        //漏洞库xml解析入库
      case "save_vulnerability" => AggrWithVulnerability.saveVulnerability(args(1), args(2), args(3), args(4), args(5))

      case "load_ids"           => LoadAggrLog.loadIDSLog(args(1))
    }
  }

}
