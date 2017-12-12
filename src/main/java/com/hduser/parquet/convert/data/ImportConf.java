package com.hduser.parquet.convert.data;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ImportConf {
	public String hdfsURL="hdfs://localhost:54310/user/hduser/parquet/";

	public String sqlURL="jdbc:sqlserver://localhost;databaseName=eHRM_Hamaden;user=sa;password=Khanhno1;";
	public String getHdfsURL() {
		return hdfsURL;
	}

	public void setHdfsURL(String hdfsURL) {
		this.hdfsURL = hdfsURL;
	}

	public String getSqlURL() {
		return sqlURL;
	}

	public void setSqlURL(String sqlURL) {
		this.sqlURL = sqlURL;
	}

	public SparkSession spark = SparkSession
			.builder()
			.master("local[*]")
			.appName("Java Spark SQL Parquet")
			.getOrCreate();

	public Dataset<Row> loadTableJDBC(String table) {
		Dataset<Row> dataset = spark.read().format("jdbc")
				.option("url", sqlURL)
				.option("dbtable", table)
				.load();
		return dataset;
	}

	public void importTable(String table) {
			Dataset<Row> periods = loadTableJDBC(table);
			periods.write().parquet(hdfsURL+table);
	}


	public void importTable(String talbe,String col, String... cols ) throws AnalysisException{
		Dataset<Row> periods = loadTableJDBC(talbe).select(col,cols);
		periods.write().parquet(hdfsURL+talbe);
		
	}

}
