package com.spark.scala

import java.sql.DriverManager

import com.spark.scala.Test.MASTER
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Stream {
  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")

    val sc = new StreamingContext(sparkConf, Seconds(1))
   // StreamingExamples.setStreamingLogLevels()
    val lines = sc.socketTextStream("192.168.44.131",10001)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    val outputFile = "E:\\data\\out\\a.txt"
    wordCounts.foreachRDD(rdd =>{
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          System.out.println( record._1 + "'," + record._2  )
          val sql = "insert into wordcount(word, wordcount) values('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
        })
        connection.close()
      })
    })
  //  wordCounts.repartition(1).saveAsTextFiles(outputFile)

    sc.start()
    sc.awaitTermination()
  }

  def createConnection() = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=utf8&serverTimezone=UTC", "root", "123456")
  }
}
