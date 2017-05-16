package com.sparklearning.com.spark

  import org.apache.spark.sql._
  import org.apache.log4j._

/**
  * Created by ke20506 on 2/22/2017.
  */
object SparkSqlDatasets {

  def main (args: Array[String]) {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp")
    .appName("sparkSqlDatasets")
    .getOrCreate()

  val lines = spark.read.option("header","true").option("inferSchema","true").csv("C:/Users/ke20506/Desktop/Projects/spark/SparkScala/fakefriends.csv")

  lines.printSchema()
  }
}