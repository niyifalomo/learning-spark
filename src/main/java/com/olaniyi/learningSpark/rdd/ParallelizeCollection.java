package com.olaniyi.learningSpark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Convert a collection items to form a distributed data-set that can be operated in parallel
 */
public class ParallelizeCollection
{
	public static void main(String[] args)
	{
		String appName = "Parallelize Collection";
		String clusterUrl = "local";
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(clusterUrl);
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		List<Integer> data = Arrays.asList(2, 4, 2, 3, 4, 6);
		// convert data to distributed data
		JavaRDD<Integer> distData = sparkContext.parallelize(data);
		// add all elements in the distributed data
		int sum = distData.reduce((a, b) -> a + b);

	}
}

