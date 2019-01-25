package com.olaniyi.learningSpark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Count the words in a text file
 */
public class WordCount_1
{
	public static void main(String[] args)
	{
		String appName = "Word Count";
		String clusterUrl = "local";
		String inputFile = "input/log2.txt";

		SparkConf conf = new SparkConf().setAppName(appName).setMaster(clusterUrl);
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		JavaRDD<String> distData = sparkContext.textFile(inputFile);
		int wordCount = distData.map(s -> s.split(" ").length).reduce((a, b) -> (a + b));

		System.out.printf("Total number of words: %d\n", wordCount);

	}
}
