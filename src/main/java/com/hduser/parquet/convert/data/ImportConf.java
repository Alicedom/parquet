package com.hduser.parquet.convert.data;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class ImportConf {
	/*
	 * link to hdfs
	 * hdfs://<master ip>:<master port>/path/to/database
	 */
	public String hdfsURL="/home/hduser/Doccuments/script/data/parquet2/";

	/*
	 * Connection String to connect sql server 
	 */
	public String sqlURL="jdbc:sqlserver://localhost;databaseName=eHRM_Hamaden;user=sa;password=Khanhno1;";
	
	public SparkSession spark = SparkSession
			.builder()
			.master("local[*]")
			.appName("Java Spark SQL Parquet")
//			.config("spark.eventLog.enabled",true)
//			.config("spark.eventLog.dir","/usr/local/hadoop_store/tmp")
			.getOrCreate();

	public Dataset<Row> loadTableJDBC(String table) {
		Dataset<Row> dataset = spark.read().format("jdbc")
				.option("url", sqlURL)
				.option("dbtable", table)
				.load();
		return dataset;
	}

	public void importTable(String table) throws AnalysisException {
		Dataset<Row> dataset = loadTableJDBC(table);
		dataset.repartition(4).write().mode(SaveMode.Overwrite).parquet(hdfsURL+table);
	}


	public void importTable(String table,String col, String... cols ) throws AnalysisException {
		Dataset<Row> dataset = loadTableJDBC(table).select(col,cols);
		dataset.repartition(4).write().mode(SaveMode.Overwrite).parquet(hdfsURL+table);
	}

}
