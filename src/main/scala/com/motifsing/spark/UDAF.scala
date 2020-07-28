package com.motifsing.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/***
 * @author yhw
 * 2020/7/24 14:45
 * data:
 * {"name":"lillcol", "age":24,"ip":"192.168.0.8"}
 * {"name":"adson", "age":100,"ip":"192.168.255.1"}
 * {"name":"wuli", "age":39,"ip":"192.143.255.1"}
 * {"name":"gu", "age":20,"ip":"192.168.255.1"}
 * {"name":"ason", "age":15,"ip":"243.168.255.9"}
 * {"name":"tianba", "age":1,"ip":"108.168.255.1"}
 * {"name":"clearlove", "age":25,"ip":"222.168.255.110"}
 * {"name":"clearlove", "age":30,"ip":"222.168.255.110"}
 * @return
 */



object UDAF extends UserDefinedAggregateFunction {
  // 聚合函数输入参数的数据类型
  // :: 用于的是向队列的头部追加数据，产生新的列表,Nil 是一个空的 List，定义为 List[Nothing]
  override def inputSchema: StructType = StructType(StructField("age", IntegerType) :: Nil)

  //等效于
  //  override def inputSchema: StructType=new StructType() .add("age", IntegerType).add("name", StringType)

  // 聚合缓冲区中值的数据类型
  override def bufferSchema: StructType = {
    StructType(StructField("sum", IntegerType) :: StructField("count", IntegerType) :: Nil)
  }

  // UserDefinedAggregateFunction返回值的数据类型。
  override def dataType: DataType = DoubleType

  // 如果这个函数是确定的，即给定相同的输入，总是返回相同的输出。
  override def deterministic: Boolean = true

  //  初始化给定的聚合缓冲区，即聚合缓冲区的零值。
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // sum，  总的年龄
    buffer(0) = 0
    // count， 人数
    buffer(1) = 0
  }

  // 使用来自输入的新输入数据更新给定的聚合缓冲区。
  // 每个输入行调用一次。（同一分区）
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getInt(0) + input.getInt(0) //年龄 叠加
    buffer(1) = buffer.getInt(1) + 1 //人数叠加
  }

  //  合并两个聚合缓冲区并将更新后的缓冲区值存储回buffer1。
  // 当我们将两个部分聚合的数据合并在一起时，就会调用这个函数。（多个分区）
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0) //年龄 叠加
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1) //人数叠加
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getInt(0).toDouble / buffer.getInt(1)
  }

  Logger.getLogger("org").setLevel(Level.WARN)

  val spark: SparkSession = SparkSession.builder().appName("test").master("local[2]").getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    spark.udf.register("myAvg", UDAF)
    val ds: Dataset[Row] = spark.read.json("C:\\Users\\Administrator\\Desktop\\testJson.json")
    ds.createOrReplaceTempView("table1")
    //SQL
    spark.sql("select myAvg(age) as avgAge fro       table1").show()

    ds.select(UDAF($"age").as("avgAge")).show()
  }
}
