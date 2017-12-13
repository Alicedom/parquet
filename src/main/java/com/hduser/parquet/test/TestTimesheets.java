package com.hduser.parquet.test;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.hduser.parquet.timesheet.Conf;
import com.hduser.parquet.timesheet.Periods;




public class TestTimesheets {
	
	private Dataset<Row> timesheet_period;
	
	public TestTimesheets() {
		Conf.loadTable("TA_EMPLOYEE_TIMESHEETS");
		Conf.loadTable("TA_WORKING_SHIFTS");
		Conf.loadTable("TA_WORKING_TYPES");
	}
	
	public TestTimesheets(String start_date, String end_date) {
		new TestTimesheets();
		getTimesheetPeriod(start_date, end_date);

	}

	public Dataset<Row> getTimesheetPeriod(String start_date, String end_date) {

		String sql_get_time_sheet_period = 
				"select *"+
				" from TA_EMPLOYEE_TIMESHEETS as Sheet "
				+ "join TA_WORKING_SHIFTS as Shift on Sheet.ACTUAL_SHIFT_ID = Shift.WORKING_SHIFT_ID "
				+ "join TA_WORKING_TYPES as Type on Sheet.APPROVED_WORKING_TYPE_ID =  Type.WORKING_TYPE_ID";
		this.timesheet_period = Conf.spark.sql(sql_get_time_sheet_period)
				.filter(col("WORKING_DATE").between(start_date, end_date));

		//		timesheet_period.write().mode(SaveMode.Overwrite).json("/home/hduser/out/timesheet");
		//		timesheet_period.cache();
		return timesheet_period;
	}

	
	public Dataset<Row> getTimesheetEmployee(Dataset<Row> dataset, int employee_id){
		return dataset.filter(col("EMPLOYEE_ID").equalTo(employee_id));
		
	}
	public Dataset<Row> getTimesheetEmployee(int employee_id){
		return getTimesheetEmployee(timesheet_period, employee_id);
		
	}

	public Dataset<Row> filterInNight(Dataset<Row> dataset){
		return dataset.filter(col("IS_NIGHT_SHIFT").equalTo(1));
		
	}
	public Dataset<Row> filterInNight(){
		return filterInNight(timesheet_period);
		
	}

	public Dataset<Row> groupbyWorkingType(Dataset<Row> dataset) {
		return dataset.groupBy("ACTUAL_SHIFT_ID","EMPLOYEE_ID","COEFFICIENT","IS_NIGHT_SHIFT","WORKING_TYPE_NAME").count();
		
	}
	public Dataset<Row> groupbyWorkingType() {
	 return groupbyWorkingType(timesheet_period);
	}
	
	public Dataset<Row> calculateSalary(Dataset<Row> dataset) {
		return dataset.select(col("COEFFICIENT").multiply(col("count")));
		
	}

	public Dataset<Row> timesheetv1(int period){
		String sql ="select\n" + 
				"	EMPLOYEE_ID,\n" + 
				"	APPROVED_WORKING_TYPE_ID,\n" + 
				"	PERIOD_ID,\n" + 
				"	count(s.EMPLOYEE_ID) as NUMBER_DATE\n" + 
				"\n" + 
				"from\n" + 
				"	(select START_DATE,END_DATE,PERIOD_ID\n" + 
				"	from 	PERIODS\n" + 
				"	where	PERIOD_ID ="+ period+
				" ) p,\n" + 
				"	TA_EMPLOYEE_TIMESHEETS s\n" + 
				"\n" + 
				"where\n" + 
				"	s.APPROVED = 1\n" + 
				"	and APPROVED_WORKING_TYPE_ID IS NOT NULL\n" + 
				"	and s.WORKING_DATE between p.START_DATE and p.END_DATE\n" + 
				"group by \n" + 
				"	EMPLOYEE_ID,\n" + 
				"	APPROVED_WORKING_TYPE_ID,\n" + 
				"	PERIOD_ID\n" + 
				"\n" + 
				"order by EMPLOYEE_ID	\n" + 
				"";
		return Conf.spark.sql(sql);
	}
	public static void main(String[] args) {
		int period_id = 89;
//		int employee_id = 5170;
		long start,stop;
		
		start = System.currentTimeMillis();
		Periods periods = new Periods();
		String start_date = periods.getStartDate(period_id);
		String end_date = periods.getEndDate(period_id);
		TestTimesheets timesheet = new TestTimesheets(start_date, end_date);
		timesheet.groupbyWorkingType().orderBy("EMPLOYEE_ID").repartition(1).write().mode(SaveMode.Overwrite).json("/home/hduser/out/groupbyWorkingType4");;
		stop = System.currentTimeMillis();

		long time1 = stop - start;
		
		start = System.currentTimeMillis();
		new Periods();
		timesheet = new TestTimesheets();
		timesheet.timesheetv1(89).orderBy("EMPLOYEE_ID").repartition(1).write().mode(SaveMode.Overwrite).json("/home/hduser/out/testv3");
		stop = System.currentTimeMillis();

		long time2 = stop - start;
		
		start = System.currentTimeMillis();
		start_date = periods.getStartDate(period_id);
		end_date = periods.getEndDate(period_id);
		timesheet = new TestTimesheets(start_date, end_date);
		timesheet.groupbyWorkingType().orderBy("EMPLOYEE_ID").repartition(1).write().mode(SaveMode.Overwrite).json("/home/hduser/out/groupbyWorkingType4");;
		stop = System.currentTimeMillis();
		
		long time3 = stop - start;
		
		start = System.currentTimeMillis();
		new Periods();
		timesheet = new TestTimesheets();
		timesheet.timesheetv1(89).orderBy("EMPLOYEE_ID").repartition(1).write().mode(SaveMode.Overwrite).json("/home/hduser/out/testv3");
		stop = System.currentTimeMillis();

		long time4 = stop - start;

		
		start = System.currentTimeMillis();
		start_date = periods.getStartDate(period_id);
		end_date = periods.getEndDate(period_id);
		timesheet = new TestTimesheets(start_date, end_date);
		timesheet.groupbyWorkingType().orderBy("EMPLOYEE_ID").repartition(1).write().mode(SaveMode.Overwrite).json("/home/hduser/out/groupbyWorkingType4");;
		stop = System.currentTimeMillis();
		
		long time5 = stop - start;
		
		start = System.currentTimeMillis();
		new Periods();
		timesheet = new TestTimesheets();
		timesheet.timesheetv1(89).orderBy("EMPLOYEE_ID").repartition(1).write().mode(SaveMode.Overwrite).json("/home/hduser/out/testv3");
		stop = System.currentTimeMillis();

		long time6 = stop - start;

		
		start = System.currentTimeMillis();
		start_date = periods.getStartDate(period_id);
		end_date = periods.getEndDate(period_id);
		timesheet = new TestTimesheets(start_date, end_date);
		timesheet.groupbyWorkingType().orderBy("EMPLOYEE_ID").repartition(1).write().mode(SaveMode.Overwrite).json("/home/hduser/out/groupbyWorkingType4");;
		stop = System.currentTimeMillis();
		
		long time7 = stop - start;
		
		start = System.currentTimeMillis();
		new Periods();
		timesheet = new TestTimesheets();
		timesheet.timesheetv1(89).orderBy("EMPLOYEE_ID").repartition(1).write().mode(SaveMode.Overwrite).json("/home/hduser/out/testv3");
		stop = System.currentTimeMillis();

		long time8 = stop - start;

		System.out.println(time1+"\n"+time2+"\n"+time3+"\n"+time4+"\n"+time5+"\n"+time6+"\n"+time7+"\n"+time8);
		
		Conf.spark.close();
	}



}

