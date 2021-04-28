package com.motifsing.course.scala

object MyWhile {
  def main(args: Array[String]): Unit = {
    var n = 5
    myWhile(n >= 1){
      println(n)
      n -= 1
    }
    n = 5
    myWhile2(n >= 1){
      println(n)
      n -= 1
    }
  }

  // 用闭包和传名参数（控制抽象）实现自定义while，将代码块作为参数传入，递归调用
  def myWhile(condition: =>Boolean): (=>Unit)=>Unit = {
    // 内层函数需要递归调用，参数就是循环体
    def doLoop(op: =>Unit): Unit ={
      if (condition) {
        op
        myWhile(condition)(op)
      }
    }
    doLoop
  }

  def myWhile2(condition: =>Boolean): (=>Unit)=>Unit = {
    // 内层函数需要递归调用，参数就是循环体
    op => {
      if (condition) {
        op
        myWhile(condition)(op)
      }
    }
  }

  def myWhile3(condition: =>Boolean)(op: =>Unit): Unit ={
    if (condition) {
      op
      myWhile(condition)(op)
    }
  }

}
