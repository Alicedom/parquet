package com.hduser.parquet.timesheet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Conf {
	public static String hdfsURL = new com.hduser.parquet.convert.data.ImportConf().hdfsURL;
    public static SparkSession spark = SparkSession
    	      .builder()
    	      .master("local[*]")
    	      .appName("Java Spark SQL basic example")
//    	      .config("spark.some.config.option", "some-value")
    	      .getOrCreate();
    public static String outURL = "/home/hduser/Documents/script/SQL/out/";
    public static final int BASIC_SALARY_ID = 46; 
	
    /*
     * load parquet data and create tempview
     * to request 
     */
    public static void loadTable(String table) {

		Dataset<Row> parquetFileDF = Conf.spark.read().parquet(Conf.hdfsURL+table);
		parquetFileDF.createOrReplaceTempView(table);
    }

}
