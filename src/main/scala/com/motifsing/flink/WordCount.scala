package com.motifsing.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.kafka.clients.consumer.ConsumerConfig


object WordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.20.1.1:9092,172.20.1.2:9092,172.20.1.3:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "FlinkTest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    val topicName = "yhw_test"

    val consumer = new FlinkKafkaConsumer[String](topicName, new SimpleStringSchema(), props)
//    consumer.setStartFromGroupOffsets()  // 默认的方法

    // 指定位置
    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()

    env.createInput()

    specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 0L)
    specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 0L)
    specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 0L)

    consumer.setStartFromSpecificOffsets(specificStartOffsets)

    val dataStream = env.addSource(consumer)

    dataStream.map(record => {
      val words = record.split("\t")
      val ele = words(0)
      val dTime = words(1)
      (ele, dTime)
    })

    dataStream.print()
    env.execute(WordCount.getClass.getName)
  }
}
