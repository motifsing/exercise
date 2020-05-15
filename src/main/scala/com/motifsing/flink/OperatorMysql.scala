package com.motifsing.flink

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.types.Row

object OperatorMysql {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://127.0.0.1:3306/test"
    val username = "root"
    val password = "123456"
    val sql_read = "select topic, topic_partition, kafka_offset from t_kafka_offset_info_monitor where monitor_type='flink_demo'"
    readMysql(env, url, driver, username, password, sql_read)
  }

  def readMysql(env: ExecutionEnvironment, url: String, driver: String, user: String, pwd: String, sql: String): Unit = {
    val dataResult: DataSet[Row] = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(driver)
      .setDBUrl(url)
      .setUsername(user)
      .setPassword(pwd)
      .setQuery(sql)
      .setRowTypeInfo(new RowTypeInfo(
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO))
      .finish())

    dataResult.map(x => {
      val topic = x.getField(0)
      val partition = x.getField(1)
      val offset = x.getField(2)
      (topic, partition, offset)
    }).print()
  }
}
