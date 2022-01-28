package com.spark.scala.learning.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import java.util.Properties
import scala.io.Source

object LoadCSV {
  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit ={

    logger.info("Starting Spark Session")
    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()

    val surveyDF = loadSurveyDF(spark, args(0))
    val partitionSurveyDF = surveyDF.repartition(2)
    val countByCountryDF = countByCountry(partitionSurveyDF)

    countByCountryDF.show()
    logger.info("Stoping Spark Session")
    spark.stop()
  }

  def loadSurveyDF(spark: SparkSession, dataFile:String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataFile)
  }

  def countByCountry(surveyDF: DataFrame): DataFrame = {
    surveyDF.where("Age < 40")
      .select("Country", "Gender")
      .groupBy("Country")
      .count()
  }

  def getSparkAppConf : SparkConf ={
    val sparkAppConf = new SparkConf
    val props = new Properties
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k,v) => sparkAppConf.set(k.toString, v.toString))
    //This is a fix for Scala 2.11
    //import scala.collection.JavaConverters._
    //props.asScala.foreach(kv => sparkAppConf.set(kv._1, kv._2))
    sparkAppConf
  }
}
