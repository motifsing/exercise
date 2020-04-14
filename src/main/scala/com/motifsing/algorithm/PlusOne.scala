package com.motifsing.algorithm

object PlusOne {
  def main(args: Array[String]): Unit = {
    val a = Array(4, 5, 6)
    val b = plusOne(a)
    println(b)
  }

  def plusOne(digits:Array[Int])={
    var i = null
    for (i <- Range(digits.length-1, -1, -1)){
      digits(i) += 1
      digits(i) %= 10
      if (digits(i) != 0)
        digits
    }
    if (i == 0){
      val new_arr = Array(1)
      for(j <- 1 to digits.length)
        0 +: new_arr
      new_arr
    }
  }

}
