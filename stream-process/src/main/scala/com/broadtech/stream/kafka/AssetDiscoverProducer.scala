package com.broadtech.stream.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @author jiangqingsong
 * @description 测试发送批量数据时使用
 * @date 2020-06-05 10:51
 */

case class AssetDiscoverCase(resource_name: String, task_id: String, scan_time: String, resource_info: Array[String])

object AssetDiscoverProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("asset_discover_scan")
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "master02:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //定义一个kafka producer
    val producer = new KafkaProducer[String, String](properties)

    //从文件中读取数据并发送
    val bufferedSource = io.Source.fromFile("D:\\SDC\\gitlab_code\\sdcplatform\\SDCPlatform\\stream-process\\src\\main\\resources\\asset_discover.txt")
    /*for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      println(line)
      producer.send(record)
    }*/
    val record = new ProducerRecord[String, String](topic, "{\"ResourceInfo\":[\"192.168.7.1\",\"192.168.7.2\",\"192.168.7.3\",\"192.168.7.4\",\"192.168.7.5\",\"192.168.7.6\",\"192.168.7.7\",\"192.168.7.10\",\"192.168.7.12\",\"192.168.7.13\",\"192.168.7.14\",\"192.168.7.15\",\"192.168.7.16\",\"192.168.7.17\",\"192.168.7.18\",\"192.168.7.19\",\"192.168.7.92\",\"192.168.7.230\",\"192.168.7.231\",\"192.168.7.233\",\"192.168.7.234\",\"192.168.7.235\",\"192.168.7.236\",\"192.168.7.237\",\"192.168.7.238\",\"192.168.7.248\",\"192.168.7.249\",\"192.168.7.250\",\"192.168.7.251\",\"192.168.7.252\"],\"ResourceName\":\"OnlineIPTable\",\"ScanTime\":\"2020-06-15 22:58:00\",\"TaskID\":\"1636dd26d8884dcfb6800e9407209817\"}")
    while (true) {
      producer.send(record)
      println("111")
      Thread.sleep(1000);
    }
    producer.close()
  }
}
