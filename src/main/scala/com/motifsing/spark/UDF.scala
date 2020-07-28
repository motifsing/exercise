package com.motifsing.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}

/**
 * @author yhw
 * 2020/7/24 14:46
 * @return
 */

object UDF {

  Logger.getLogger("org").setLevel(Level.WARN)

  val spark: SparkSession = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
  val sQLContext: SQLContext = spark.sqlContext

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    testUdf()
  }

  def testUdf(): Unit = {
    val iptoLong: UserDefinedFunction = getIpToLong
    val ds: Dataset[Row] = spark.read.json("C:\\Users\\Administrator\\Desktop\\testJson.json")
    ds.createOrReplaceTempView("table1")
    sQLContext.udf.register("addName", sqlUdf(_: String)) //addName 只能在SQL里面用  不能在DSL 里面用
    //1.SQL
    sQLContext.sql("select *,addName(name) as nameAddName  from table1")
      .show()
    //2.DSL
    val addName: UserDefinedFunction = udf((str: String) => "ip: " + str)
    ds.select($"*", addName($"ip").as("ipAddName"))
      .show()

    //如果自定义函数相对复杂，可以将它分离出去 如iptoLong
    ds.select($"*", iptoLong($"ip").as("iptoLong"))
      .show()
  }

  def sqlUdf(name: String): String = {
    "name:" + name
  }

  /**
   * 用户自定义 UDF 函数
   *
   * @return
   */

  def getIpToLong: UserDefinedFunction = {
    val ipToLong: UserDefinedFunction = udf((ip: String) => {
      val arr: Array[String] = ip.replace(" ", "").replace("\"", "")
        .split("\\.")
      var result: Long = 0
      var ipl: Long = 0
      if (arr.length == 4) {
        for (i <- 0 to 3) {
          ipl = arr(i).toLong
          result |= ipl << ((3 - i) << 3)
        }
      } else {
        result = -1
      }
      result
    })

    ipToLong

  }


}
