package com.hduser.parquet.timesheet;

import org.apache.spark.sql.Encoders;

public class Periods {
	public Periods() {
		Conf.loadTable("PERIODS");
	}
	public String getStartDate(int period_id) {

		String sql_get_start_date= "select START_DATE from PERIODS where PERIOD_ID = "+period_id; 
		String start_date= Conf.spark.sql(sql_get_start_date).as(Encoders.STRING()).first();
		
		return start_date;
	}

	public String getEndDate(int period_id) {

		String sql_get_end_date="select END_DATE from PERIODS where PERIOD_ID = "+period_id;
		String end_date= Conf.spark.sql(sql_get_end_date).as(Encoders.STRING()).head();

		return end_date;
	}
	public static void main(String[] args) {
		Periods period = new Periods();
		System.out.println(period.getStartDate(89));
		System.out.println(period.getEndDate(89));
		Conf.spark.close();
	}

}