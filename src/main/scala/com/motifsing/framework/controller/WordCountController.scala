package com.motifsing.framework.controller

import com.motifsing.framework.common.TController
import com.motifsing.framework.service.WordCountService

/**
 * 控制层
 */
class WordCountController extends TController{

  private val wordCountService = new WordCountService()

  // 调度
  def dispatch(): Unit = {
    val array = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
