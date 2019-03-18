package com.spark.scala

object Vali {
  def main(args: Array[String]): Unit = {
    def f = {println("hellow");1.0}

    val d = {println("hello1");1.0}

    lazy val m = {println("hello2");1.0}


    def callByValue(x: Int) = {
      println("x1=" + x)
      println("x2=" + x)
    }

    //val x = 1
    //callByValue(x)

    def callByName(x: =>Int) = {
      println("x33=" + x)
      println("x44=" + x)
    }

   // callByName(x)

    def func() = {
      println("func")
      1
    }

    //callByValue(func())
    callByName(func())


    val myMap: Map[String, String] = Map("key1" -> "value")
    val value1: Option[String] = myMap.get("key1")
    val value2: Option[String] = myMap.get("key2")

    println("*********************")
    println(value1) // Some("value1")
    println(value2) // None
  }

}
