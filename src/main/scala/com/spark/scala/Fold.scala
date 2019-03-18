package com.spark.scala

object Fold {

  def main(args: Array[String]): Unit = {

    val list = List(1,2,3,4,5)
    println(list.fold(1)((a,b)=>a*b))

  }

}
