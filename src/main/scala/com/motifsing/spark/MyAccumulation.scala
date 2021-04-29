package com.motifsing.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

object MyAccumulation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
    conf.setAppName("MyAccumulationTest")
    conf.setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.makeRDD(List("spark", "flink", "java", "spark"))

    val myAcc = new MyAcc()
    sc.register(myAcc, "myAcc")

    rdd.foreach(word => {
      myAcc.add(word)
    })

    println(myAcc.value)

    sc.stop()
  }

  class MyAcc extends AccumulatorV2[String, mutable.Map[String, Long]] {

    private val wcMap = mutable.Map[String, Long]()

    // 判断是否为初始状态
    override def isZero: Boolean = wcMap.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAcc()

    override def reset(): Unit = wcMap.clear()

    // 获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCount = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCount)
    }

    // Driver端合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach({
        case (word, count) =>
        val newCount = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
      })
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = wcMap
  }

}
