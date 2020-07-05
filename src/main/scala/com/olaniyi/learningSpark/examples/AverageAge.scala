package com.olaniyi.learningSpark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

/**
 * Get the average of ages in a DF grouped by names.
 */
object AverageAge {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("AverageAge")
      .config("spark.master", "local")
      .getOrCreate()

    val authorsDf = spark.createDataFrame(Seq(("Mark", 10), ("James", 15), ("Mark", 8)
      , ("Paul", 33), ("Mark", 2))).toDF("Name", "Age")

    val averageDF = authorsDf.groupBy("Name")
      .agg(avg("Age"))
    averageDF.show();

    spark.stop()
  }

}
