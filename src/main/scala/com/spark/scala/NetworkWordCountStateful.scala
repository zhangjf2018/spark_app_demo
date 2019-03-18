package com.spark.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCountStateful {
  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("NetworkWordCountStateful").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("file:\\E:\\data\\checkpoint")

    val sd = ssc.socketTextStream("192.168.44.131",9999)
    val count = sd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      println(values)
      println("----------------")
      println(state)
      println("----------------")
      val currentCount = values.fold(0)(_+_)
      val historyCount = state.getOrElse(0)

      Some(currentCount + historyCount)
    }
    val stateStream = count.updateStateByKey[Int](updateFunc)
    stateStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
