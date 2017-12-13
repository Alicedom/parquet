package com.hduser.parquet.convert.data;

import org.apache.spark.sql.AnalysisException;

public class ConvertParquet {
	public ConvertParquet() throws AnalysisException {
		
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
	
	public static void main(String[] args) {
		try {
			new ConvertParquet();
		} catch (AnalysisException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
