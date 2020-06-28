package com.motifsing.kafka

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._


object KafkaOffsetFind {
  def main(args: Array[String]): Unit = {
    val topic = "ssp_etl_logs"
    val kafkaCluster = "172.20.2.30:19092,172.20.2.31:19092,172.20.2.32:19092"
    val group_id = "StreamingSSPStatTest"
    val offsetFinder = new KafkaOffsetFind[String]
    val tm = "2020-06-19 23:59:59"
    val timestamp = tranTimeToLong(tm)
    val offset = offsetFinder.useTimestamp(timestamp,topic,kafkaCluster,group_id)
    print(offset)
  }

  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm.parse(tm)
    val tim: Long = dt.getTime
    tim
  }

}


class KafkaOffsetFind[T] {
  //超时时间
  val POLL_TIMEOUT = 2000

  //使用时间查询
  def useTimestamp(timestamp: Long, topic: String,kafkaCluster:String,group_id:String): List[(Int, Long)] = {

    //创建消费者,获得消费者分区
    val consumer = createConsumer(kafkaCluster,group_id)
    consumer.subscribe(util.Arrays.asList(topic))
    consumer.poll(POLL_TIMEOUT)
    val partitions = consumer.assignment().asScala.toList

    //拼出一个查询map
    val findMap = new util.HashMap[TopicPartition, java.lang.Long]
    partitions
      .foreach {
        c =>
          println(c)
          findMap.put(new TopicPartition(topic, c.partition()), timestamp)
      }

    //使用查询map去获得偏移量
    val offsetMap = consumer.offsetsForTimes(findMap)

    //返回前关闭下消费者
    consumer.close()

    //返回分区号和对应的偏移量
    partitions.map {
      p =>
        (p.partition(), offsetMap.get(new TopicPartition(topic, 0)).offset())
    }
  }

  //创建消费者
  protected def createConsumer(kafkaCluster:String,group_id:String): KafkaConsumer[String, T] = {

    val props = new Properties()
    props.setProperty("bootstrap.servers", kafkaCluster)
    props.setProperty("group.id", group_id)
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    new KafkaConsumer[String, T](props)
  }

}



