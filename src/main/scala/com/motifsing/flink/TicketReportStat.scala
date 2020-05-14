package com.motifsing.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @author yhw
 * 2020/4/21 16:41
 */

object TicketReportStat {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    var properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.20.1.1:9092,172.20.1.2:9092,172.20.1.3:9092")
    // 仅 Kafka 0.8 需要
//    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "FlinkTest")

    val topic = "yhw_test"

    val flinkKafkaConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    flinkKafkaConsumer.setStartFromGroupOffsets()  // 默认的方法

    val dataStream = env.addSource(flinkKafkaConsumer)

    dataStream.map(values => {
      val words = values.split(" ", -1)
      val theDate = words(1).split(" ", -1)(0).replace("-", "")
      val ticketId = words(21)
      val mt = words(17)
      (theDate, ticketId, mt)
    })
//      .filter(words => "5101,6101,7102".contains(words._3)).print(_)

  }

}
