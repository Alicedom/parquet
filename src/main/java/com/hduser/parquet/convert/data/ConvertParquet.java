package com.hduser.parquet.convert.data;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.hduser.parquet.timesheet.Conf;

public class ConvertParquet {
	public ConvertParquet() {

		try {
			importAll();
		} catch (AnalysisException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void importAll() throws AnalysisException {
		
		ImportConf conf = new ImportConf();
		
		conf.importTable("PERIODS");
		//timesheet
		conf.importTable("TA_WORKING_SHIFTS");
		conf.importTable("TA_WORKING_TYPES");
		conf.importTable("TA_EMPLOYEE_TIMESHEETS","EMPLOYEE_ID","EMPLOYEE_TIMESHEET_ID","WORKING_DATE","TIME_IN","TIME_OUT","ACTUAL_SHIFT_ID","APPROVED_WORKING_TYPE_ID","APPROVED");
		//OT
		conf.importTable("TA_OVERTIME_SETTING");
		conf.importTable("TA_EMPLOYEE_OVERTIMES","EMPLOYEE_TIMESHEET_ID","OT_INCOME_ID","APPROVED_HOURS","APPROVED");
		
		//Trip
//		conf.importTable("TRIP_REQUETS","TRIP_REQUEST_ID","EMPLOYEE_ID","START_DATE","END_DATE","NO_OF_WORKING_DAYS","TRIP_REQUEST_STATUS_ID");
//		conf.importTable("APPROVAL_TRIPCOSTS","EMPLOYEE_ID","DATE","IS_HALF_DAY","ALLOWANCE_ID","TOTAL","IS_APPROVAL","TRIP_REQUEST_ID");
//		conf.importTable("TRIP_COST_LOCATION");
		//Leave
		conf.importTable("LEAVE_REQUESTS","LEAVE_REQUEST_ID","LEAVE_TYPE_ID","EMPLOYEE_ID","START_DATE","END_DATE","NO_OF_WORKING_DAYS","NO_OF_CALENDAR_DAYS","LEAVE_REQUEST_STATUS_ID","COUNT_APPROVE");
			
		//Dependent
		conf.importTable("EMPLOYEES","EMPLOYEE_ID","PROBATION_END_DATE");
		conf.importTable("EMPLOYEE_DEPENDANTS");
		conf.importTable("EMPLOYEE_RELATIVES");
		//Tax
		conf.importTable("TAXES");
		conf.importTable("INCOME_CONFIGS","INCOME_CONFIG_ID","EMPLOYEE_ID","INCOME_ID","CUSTOM_VALUE","START_DATE","END_DATE");
		
	}
	String sql_working_date = 
			"select \n" + 
			"	PERIOD_ID,\n" + 
			"	START_DATE,\n" + 
			"	END_DATE,\n" + 
			"	stage_1 + stage_2 - stage_3 as WORKING_DATE\n" + 
			"\n" + 
			"from\n" + 
			"(	select\n" + 
			"		*,\n" + 
			"		case when datediff(mo1,START_DATE) -2 > 0 then datediff(mo1,START_DATE) -2 else 0 end as stage_1, \n" + 
			"		datediff(mo2,mo1) /7 * 5 as stage_2,\n" + 
			"		case when datediff(mo2,END_DATE) -3 > 0 then datediff(mo2,END_DATE) -3 else 0 end  as stage_3 \n" + 
			"	from	 \n" + 
			"		(\n" + 
			"		select\n" + 
			"			PERIOD_ID,\n" + 
			"			START_DATE,\n" + 
			"			END_DATE,\n" + 
			"			next_day(START_DATE,'MO') as mo1,\n" + 
			"			next_day(END_DATE,'MO') as mo2,\n" + 
			"			weekofyear(START_DATE) as w1,\n" + 
			"			weekofyear(END_DATE) as w2\n" + 
			"\n" + 
			"		from\n" + 
			"			PERIODS\n" + 
			"		) t\n" + 
			") k";
	public void getTA_Working_Date() {
		Dataset<Row> workingDate = Conf.spark.sql(sql_working_date);
		workingDate.repartition().write().mode(SaveMode.Overwrite).json(Conf.hdfsURL+"TA_WORKING_CALENDAR");
	}
	
	public static void main(String[] args) {
		Conf.loadTable("PERIODS");
		new ConvertParquet().getTA_Working_Date();
	}

}
