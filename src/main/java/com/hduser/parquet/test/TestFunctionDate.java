package com.hduser.parquet.test;

import com.hduser.parquet.timesheet.Conf;
public class TestFunctionDate {
	public TestFunctionDate() {

	}

	
	public void listDay() {
//		int datediff = Conf.spark.sql("");
	}
	
	public void unixTimeStamp() {
		Conf.loadTable("TA_EMPLOYEE_TIMESHEETS");
		Conf.spark.sql("select * from TA_EMPLOYEE_TIMESHEETS")
		.write().json(Conf.outURL+"unix_timestamp");;
	}
	//Dinh dang ngay tu thu 2-> chu nhat
	public void dayOfWeek() {
		//select date_format(my_timestamp, 'EEEE') from

		Conf.loadTable("TA_EMPLOYEE_TIMESHEETS");
		Conf.loadTable("TA_WORKING_SHIFTS");
		Conf.spark.sql("select"
				+ " unix_timestamp(TIME_IN) as unix_timestamp,"
				+ " date_format(TIME_IN, 'EEEE') as WEEK_DAY,"
				+ " second(TIME_IN) as second,"
				+ " cast(TIME_IN as int) as castbigint"
				+ " from TA_EMPLOYEE_TIMESHEETS")
		.show(10);
		Conf.spark.sql("select"
				+ " unix_timestamp(WORKING_START) as unix_timestamp,"
				+ " date_format(WORKING_START, 'EEEE') as WEEK_DAY,"
				+ " second(WORKING_START) as second,"
				+ " cast(WORKING_START as int) as castbigint"
				+ " from TA_WORKING_SHIFTS")
		.show(10);
	}
	
	public static void main(String[] args) {
		new TestFunctionDate().dayOfWeek();
	}
}
