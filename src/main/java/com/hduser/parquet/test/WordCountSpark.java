package com.hduser.parquet.test;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.io.File;
import java.lang.Iterable;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


public class WordCountSpark {
	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();

		String inputFile = "/usr/local/hadoop/share/doc/*";
		String outputFile = "outspark3";
//		new File("/home/hduser/eclipse-workspace/parquet/outspark2").mkdirs();
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		sc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive","true");
		JavaRDD<String> input = sc.textFile(inputFile);
		// Split up into words.
		JavaRDD<String> words = input.flatMap(
				new FlatMapFunction<String, String>() {
					public Iterable<String> call(String x) {
						return Arrays.asList(x.split(" "));
					}});
		// Transform into word and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(
				new PairFunction<String, String, Integer>(){
					public Tuple2<String, Integer> call(String x){
						return new Tuple2(x, 1);
					}}).reduceByKey(new Function2<Integer, Integer, Integer>(){
						public Integer call(Integer x, Integer y){ return x + y;}});
		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFile);

		long stop = System.currentTimeMillis();

		System.out.println(stop -start );
	}
}