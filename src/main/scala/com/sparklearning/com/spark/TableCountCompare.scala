package com.sparklearning.com.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

/**
  * Created by ke20506 on 2/15/2017.
  */
object TableCountCompare {

  case class tableCount(tableName: String, rowCount: Int)

  def mapper(line: String): tableCount = {

     val fields = line.split(',')

    val tblCnt: tableCount = tableCount(fields(0), fields(1).toInt)

    return tblCnt
  }

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

      val spark = SparkSession
        .builder
        .appName("SparkSQL")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp")
        .getOrCreate()

    val linesPoc = spark.sparkContext.textFile("C:/Users/ke20506/Desktop/Projects/Mediecon/Migration/query_result_ylwork.csv")
    val linesProd = spark.sparkContext.textFile("C:/Users/ke20506/Desktop/Projects/Mediecon/Migration/query_result_ylwork_prod.csv")
    val dbtblcntPoc = linesPoc.map(mapper)
    val dbtblcntProd = linesProd.map(mapper)


    import spark.implicits._
    val schemadbtblcntPoc = dbtblcntPoc.toDS()
    val schemadbtblcntProd = dbtblcntProd.toDS()

    schemadbtblcntPoc.printSchema()
    schemadbtblcntProd.printSchema()

    schemadbtblcntPoc.createOrReplaceTempView("dbtblcntPoc")
    schemadbtblcntProd.createOrReplaceTempView("dbtblcntProd")

    val poctblcnt = spark.sql("select a.tableName, a.rowCount, b.rowCount as prodRowCount from dbtblcntPoc a full outer join dbtblcntProd b on a.tableName = b.tableName where nvl(a.rowCount, 'x') != nvl(b.rowCount, 'x')")

    val results = poctblcnt.collect()

    results.foreach(println)

    spark.stop()
  }

}
