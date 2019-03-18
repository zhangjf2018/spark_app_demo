package com.spark.scala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StructStreaming {

  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("StructedStreaming").setMaster("local[*]")
    val sc = SparkSession.builder().config(conf).getOrCreate()
    import sc.implicits._
    val lines = sc.readStream.format("socket").option("host","192.168.44.131").option("port",9999).load()

    val words = lines.as[String].flatMap(_.split(" "))
    val wordCount = words.groupBy("value").count()

    val query = wordCount.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
  }
}
