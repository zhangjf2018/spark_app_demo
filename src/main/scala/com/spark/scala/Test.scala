package com.spark.scala

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}

object Test {

  val MASTER = "local[*]"
  val DELIMITER = " "
  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
  def main(args: Array[String]): Unit = {
    println("hello world")
    val sparkConf = new SparkConf().setAppName("Test").setMaster(MASTER)
    val sc = new SparkContext(sparkConf)
    val sparkSession: SparkSession = SparkSession.builder.config(conf = sparkConf).getOrCreate()
    import sparkSession.implicits._

    val rdd = sc.parallelize(List("coffee panda","happy panda","happiest panda party"))
    //rdd.flatMap(_.split(DELIMITER)).map((_,1)).reduceByKey((pre, after)=> pre+after).sortByKey().foreach(x=>println(x._1+":"+x._2))
    //rdd.map(_ + "000").foreach(println)
    println("-------------------")
    rdd.filter(_.contains("coffee")).collect().foreach(println(_))

    val numDS = sparkSession.range(5,100,5)
    numDS.orderBy($"id").show(5)
    numDS.describe().show()
    numDS.printSchema()
    val langPercentDF = sparkSession.createDataFrame(List(("Scala", 35), ("Python", 30), ("R", 15), ("Java", 20)))
    langPercentDF.printSchema()
    langPercentDF.describe().show()
    langPercentDF.show()
    println("show df")
    langPercentDF.filter($"_1".contains("Java")).show()
    val lpDF = langPercentDF.withColumnRenamed("_1","language").withColumnRenamed("_2","percent")
    println("*************DESC***************")
    lpDF.orderBy($"percent".desc).show()

    //dataframe
    val peopleInfo = sparkSession.read.json("E:\\data\\person.txt")
    peopleInfo.printSchema()

    //rdd - > dataframe

    val people = sc.textFile("E:\\data\\people.txt")


    val peopleIn = people.map(_.split(",")).map(p=>People(p(0).toInt,p(1),p(2).toInt)).toDF
    peopleIn.printSchema()
    peopleIn.show()

    val peopleRow = people.map(_.split(",")).map(p=>Row(p(0).toInt,p(1),p(2).toInt))
    val schema = StructType(StructField("id",IntegerType,true)::
      StructField("name",StringType,true)::
      StructField("age",IntegerType,true)::Nil)

    val p2 = sparkSession.createDataFrame(peopleRow, schema)
    p2.printSchema()
    p2.show()

    p2.createOrReplaceTempView("people")

    val sqlDf = sparkSession.sql("select name from people")
    println(sqlDf.count())
    sqlDf.show()

    val pInfo = sparkSession.read.json("E:\\data\\person.txt")
    val pSalar = sparkSession.read.json("E:\\data\\salary.txt")

    val info_salary = pInfo.join(pSalar, "id")
    info_salary.show()

    val info_salary1 = pInfo.join(pSalar, Seq("id","name"))
    info_salary1.show()

    val left = pInfo.join(pSalar,pInfo("id") === pSalar("id"),"left_outer")
    left.show()

    val left2 = pInfo.join(pSalar, pInfo("id")===pSalar("id") and
    pInfo("name") === pSalar("name"),"left_outer"
    )
    left2.show()

    pSalar.groupBy("salary").agg("salary"->"sum").show()
    pSalar.groupBy("salary").agg(Map("id" -> "avg","salary"->"max")).show()

    pSalar.filter($"salary">10000).show()
    println(pSalar.filter($"salary" > 10000).count())
    pSalar.select($"name",$"salary"+10000).show()
    pSalar.select(pSalar("name"),$"salary"+20000).show()
    pSalar.select($"salary".as("salary2")).show()
  }
  case class People(id:Int, name:String, age:Int)
}
