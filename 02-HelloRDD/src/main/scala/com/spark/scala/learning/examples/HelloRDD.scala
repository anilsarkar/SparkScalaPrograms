package com.spark.scala.learning.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object HelloRDD extends  Serializable {

  def main(args: Array[String]): Unit ={
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //Create Spark Context
    val sparkConf =new SparkConf().setAppName("HelloRDD").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)
    //Read CSV file
    val lineRDD = sparkContext.textFile(args(0))

    //Give it a structure and select 4 columns
    case class SurveyRecord(Age: Int, Gender: String, Country: String, State: String)
    var colsRDD = lineRDD.map(line => {
      val cols = line.split(",").map(_.trim)
      SurveyRecord(cols(1).toInt, cols(2), cols(3), cols(4))
    })

    //Apply filter
    val filteredRDD =  colsRDD.filter(r=> r.Age < 40)
    filteredRDD.collect().foreach(println)

    ///manually implement the GroupBy
    val kvRDD = filteredRDD.map(r=> (r.Country, 1))
    val countRDD = kvRDD.reduceByKey((v1, v2) => v1 + v2)
    countRDD.collect().foreach(println)

    sparkContext.stop()

  }
}
