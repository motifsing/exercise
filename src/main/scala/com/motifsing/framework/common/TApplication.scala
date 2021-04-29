package com.motifsing.framework.common

import com.motifsing.framework.util.EnvUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

  def start(master: String = "local[*]", app: String = "Application")(op: =>Unit): Unit ={

    Logger.getLogger("org").setLevel(Level.WARN)

    val sparkConf: SparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparkConf)
    EnvUtil.put(sc)
    try {
      op
    } catch {
      case ex: Exception => println(ex.getMessage)
    }

    sc.stop()
    EnvUtil.clear()
  }

}
