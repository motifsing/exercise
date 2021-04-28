package com.motifsing.algorithm

import scala.annotation.tailrec

object Fact {
  // 尾递归
  def fact(n: Int): Int = {
    @tailrec
    def loop(n: Int, currentResult: Int): Int = {
      if (n == 0) currentResult
      else loop(n - 1, currentResult * n)
    }
    loop(n, 1)
  }


}
