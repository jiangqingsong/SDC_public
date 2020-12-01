package com.broadtech.stream.kafka

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @author leo.J
 * @description 测试发送批量数据时使用
 * @date 2020-06-05 10:51
 */

object AssetAgentProducer { 
  def main(args: Array[String]): Unit = {
    writeToKafka("test1030")
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    //properties.setProperty("bootstrap.servers", "localhost:19092")
    properties.setProperty("bootstrap.servers", "master01:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //定义一个kafka producer
    val producer = new KafkaProducer[String, String](properties)

    //从文件中读取数据并发送
    while (true) {
      val bufferedSource = io.Source.fromFile("D:\\SDC\\gitlab_code\\sdcplatform\\SDCPlatform\\stream-process\\src\\main\\resources\\asset_agent.txt")
      for (line <- bufferedSource.getLines()) {
        val record = new ProducerRecord[String, String](topic, line)
        println(line)
        producer.send(record)
      }
      Thread.sleep(1000)
    }

    producer.close()
  }
}
