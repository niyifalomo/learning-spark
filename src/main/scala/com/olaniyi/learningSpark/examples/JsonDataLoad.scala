package com.olaniyi.learningSpark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object JsonDataLoad {
  /**
   * Read JSON file(/input/blogs.json) using a defined schema
   * @param args
   */

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LoadJson")
      .config("spark.master", "local")
      .getOrCreate()

    if (args.length < 1) {
      println("Provide the path to the input json file")
      System.exit(1)
    }
    //input/blogs.json
    val jsonFile = args(0)

    //1.  defining schema using Spark DataFrame API
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits",IntegerType,false),
      StructField("Campaigns",ArrayType(StringType),false)
    ))
    //2.  defining  schema using DDL
    //val schema = "Id INT,First STRING,Last STRING,Url STRING,Hits INT,Campaigns STRING"

    val blogsDF = spark.read.schema(schema).json(jsonFile)

    blogsDF.show(false)
    println(blogsDF.printSchema)
    println(blogsDF.schema)

    spark.stop()
  }

}
