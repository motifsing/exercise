package com.motifsing.framework.service

import com.motifsing.framework.common.TService
import com.motifsing.framework.dao.WordCountDao

/**
 * 服务层
 */
class WordCountService extends TService{

  private val wordCountDao = new WordCountDao()

  // 数据分析
  def dataAnalysis(): Array[(String, Int)] = {
    val lines = wordCountDao.readFile("file:///D:\\code\\Scala\\exercise\\src\\main\\resources\\wordCount")
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map((_, 1))
    val wordToSum = wordToOne.reduceByKey(_ + _)
    val array = wordToSum.collect()
    array
  }
}
