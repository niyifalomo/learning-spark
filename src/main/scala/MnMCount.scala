package com.olaniyi.learningSpark.examples


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMCount {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("MnMCount")
      .config("spark.master", "local")
      .getOrCreate()

    if (args.length < 1) {
      println("Provide path to MnM dataset")
      sys.exit(1)
    }
    val mnmFile = args(0)

    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    val countDF = mnmDF.select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    countDF.show(10)
    println(s"Total number of rows: ${countDF.count()}")

    val texasDF = mnmDF.select("State","Color","Count")
        .where(col("State")==="TX")
         .groupBy("State","Color")
        .agg(count("Count").alias("Total"))
        .orderBy(desc("Total"))

    texasDF.show(10)
    print(s"Total numbers of rows : ${texasDF.count()}")

    spark.stop()


  }

}
