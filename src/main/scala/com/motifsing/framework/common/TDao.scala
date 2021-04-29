package com.motifsing.framework.common

import com.motifsing.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

trait TDao {
  def readFile(path: String): RDD[String] = {
      EnvUtil.take().textFile(path)
  }
}
