package com.motifsing.utils

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

object DBUtil {

  def pushDataIntoMysql(topic: String, partition: Int, offset: Long, url: String, property: Properties, monitorType: String):Boolean = {
    pushDataIntoMysql("t_kafka_offset_info_monitor",topic,partition,offset,url,property,monitorType)
  }

  def pushDataIntoMysql(table: String, topic: String, partition: Int, offset: Long, url: String,
                        property: Properties, monitorType: String):Boolean = {
    var flag = false
    var conn: Connection = null
    var ps: PreparedStatement = null
    val updateSql = s"update $table set kafka_offset=?,update_time=? " +
      "where monitor_type=? and topic=? and topic_partition=?"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateTime = new Date()
    val createTime = dateFormat.format(dateTime)
    try {
      conn = DriverManager.getConnection(url, property)
      conn.setAutoCommit(false)
      ps = conn.prepareStatement(updateSql)
      ps.setLong(1, offset)
      ps.setString(2, createTime)
      ps.setString(3, monitorType)
      ps.setString(4, topic)
      ps.setInt(5, partition)
      flag = ps.execute()
      conn.commit()
    } catch {
      case e: Exception => conn.rollback()
        e.printStackTrace()
    } finally {
      if (ps != null) ps.close()
      if (conn != null) conn.close()
    }
    flag
  }

  /**
   * 获取连接参数
   * @param table 表格名称
   * @param properties mysql配置信息
   * @return
   */
  def getJDBCLinkInfo(table:String,properties: Properties): (String, String, Properties) ={
    val jdbcUrl = properties.getProperty("jdbc_url")
    val prop = new Properties()
    prop.put("user",properties.getProperty("user"))
    prop.put("password",properties.getProperty("password"))
    (table, jdbcUrl, prop)
  }

  /**
   * 加载驱动，创建连接
   * @param url mysql的url
   * @param user 用户
   * @param password 密码
   * @return
   */
  def getConnection(url:String,user:String,password:String): Connection ={
    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection(url,user,password)
    conn
  }

  /**
   * 关闭连接
   * @param conn mysql连接对象
   */
  def close(conn: Connection): Unit ={
    try {
      if (!conn.isClosed || conn != null) {
        conn.close()
      }
    }
    catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

}

class DbUtils {

}
