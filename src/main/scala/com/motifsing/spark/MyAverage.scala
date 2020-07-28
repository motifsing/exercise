package com.motifsing.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions._

/**
 * @author yhw
 * 2020/7/24 15:34
 * @return
 */

// 既然是强类型，可能有 case 类
case class Person(name: String, age: Double, ip: String)

case class Average(var sum: Double, var count: Double)

object MyAverage extends Aggregator[Person, Average, Double] {
  //  此聚合的值为零。应该满足任意b + 0 = b的性质。
  //  定义一个数据结构，保存工资总数和工资总个数，初始都为0
  override def zero: Average = {
    Average(0, 0)
  }

  //  将两个值组合起来生成一个新值。为了提高性能，函数可以修改b并返回它，而不是为b构造新的对象。
  //  相同 Execute 间的数据合并（同一分区）
  override def reduce(b: Average, a: Person): Average = {
    b.sum += a.age
    b.count += 1
    b
  }

  // 合并两个中间值。
  // 聚合不同 Execute 的结果（不同分区）
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // 计算最终结果
  override def finish(reduction: Average): Double = {
    reduction.sum.toInt / reduction.count
  }

  //  为中间值类型指定“编码器”。
  override def bufferEncoder: Encoder[Average] = Encoders.product

  //  为最终输出值类型指定“编码器”。
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  Logger.getLogger("org").setLevel(Level.WARN)

  val spark: SparkSession = SparkSession.builder().appName("test").master("local[2]").getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val ds: Dataset[Person] = spark.read.json("C:\\Users\\Administrator\\Desktop\\testJson.json").as[Person]
    ds.show()

    val avgAge = MyAverage.toColumn  /*.name("avgAge")*///指定该列的别名为avgAge
    //执行avgAge.as("columnName") 汇报org.apache.spark.sql.AnalysisException错误  别名只能在上面指定（目前测试是这样）
    ds.select(avgAge).show()
  }
}
