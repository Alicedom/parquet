package com.hduser.parquet.convert.data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.hduser.parquet.timesheet.Conf;

public class WorkingDate {

	private JavaSparkContext sc;
	private Dataset<Row> workingDate;

	public WorkingDate() {
		Conf.loadTable("PERIODS");
	}

	public void getTA_Working_Date() throws ParseException {
		LinkedList<String> list = new LinkedList<String>();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String end = "2018-12-31";
		Calendar c = Calendar.getInstance();
		String dt= "2011-01-01";;
		c.setTime(sdf.parse(dt));

		while(dt.compareTo(end)<0 ){
			c.add(Calendar.DATE, 1);
			dt = sdf.format(c.getTime());
			list.add(dt);
		};
		SparkConf conf = new SparkConf().setAppName("Test app").setMaster("local[*]");
		sc = new JavaSparkContext(conf);
		JavaRDD<String> listRDD = sc.parallelize(list,8);
		listRDD.cache();

		workingDate = null;
		workingDate.repartition(4).write().mode(SaveMode.Overwrite).parquet(Conf.hdfsURL+"TA_WORKING_CALENDAR");
		workingDate.repartition(4).write().mode(SaveMode.Overwrite).json(Conf.outURL+"WorkingDate");
	}

	public static void main(String[] args) {
		//		Dataset<Row> workingDate = new WorkingDate().getTA_Working_Date();
		//		workingDate.repartition(4).write().mode(SaveMode.Overwrite).json(Conf.outURL+"WorkingDate");
	}


}
