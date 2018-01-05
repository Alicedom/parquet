package com.hduser.parquet.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Conf {

	
	public static final int BASIC_SALARY_ID = 46; 

	public static String hdfsURL = new com.hduser.parquet.convert.data.ImportConf().hdfsURL;
	/*
	 * link local folder to save result
	 */
	public static String outURL = "/home/hduser/Doccuments/script/SQL/out2/";

	public static SparkSession spark = SparkSession
			.builder()
//			.master("local[*]")
			.appName("Java Spark SQL basic example")
			.getOrCreate();


	public static void loadTable(String table) {

		Dataset<Row> parquetFileDF = Conf.spark.read().parquet(Conf.hdfsURL+table);
		parquetFileDF.createOrReplaceTempView(table);
	}

}
