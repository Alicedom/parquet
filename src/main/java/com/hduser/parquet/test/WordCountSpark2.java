package com.hduser.parquet.test;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCountSpark2 {
	public WordCountSpark2() {
		SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		sc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive","true");
		
		JavaRDD<String> textFile = sc.textFile("/usr/local/hadoop/share/doc/");
		JavaPairRDD<char[], Integer> counts = textFile
		    .flatMap(s -> Arrays.asList(s.toCharArray()).iterator())
		    .mapToPair(word -> new Tuple2<>(word, 1))
		    .reduceByKey((a, b) -> a + b);
		counts.saveAsTextFile("outss2");
	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		new WordCountSpark2();
		long stop = System.currentTimeMillis();
		System.out.println(stop - start);
	}
}
