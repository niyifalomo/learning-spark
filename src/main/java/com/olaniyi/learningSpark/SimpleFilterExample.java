package com.olaniyi.learningSpark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Find the number of errors and info logs in log file
 * i.e number of lines starting with 'ERROR' and 'INFO'
 */
public class SimpleFilterExample {
    public static void main(String[] args){

        String logFile = "input/log.txt";
        SparkSession spark = SparkSession.builder().appName("Simple Application").master("local[1]").getOrCreate();
        Dataset<String>  logData = spark.read().textFile(logFile).cache();

        long numErrors = logData.filter(line -> line.startsWith("ERROR")).count();
        long numInfos = logData.filter(line -> line.startsWith("INFO")).count();

        System.out.format("-Lines Starting with ERROR: %d%n-Lines with INFO: %d%n", numErrors,numInfos);
        spark.stop();


    }
}
