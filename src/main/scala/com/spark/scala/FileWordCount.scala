package com.spark.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWordCount {

  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FileWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(10))

    val fileStream = ssc.textFileStream("E:\\data\\stream")
    fileStream.map(lines=>println(lines)).print()
    val count = fileStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    count.foreachRDD(rdd=>{
      rdd.foreach(println(_))
    })
    count.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
