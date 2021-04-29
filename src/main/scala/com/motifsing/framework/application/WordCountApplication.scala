package com.motifsing.framework.application

import com.motifsing.framework.common.TApplication
import com.motifsing.framework.controller.WordCountController

object WordCountApplication extends App with TApplication{
  // 启动应用程序
  start() {
    val controller = new WordCountController()
    controller.dispatch()
  }

}
