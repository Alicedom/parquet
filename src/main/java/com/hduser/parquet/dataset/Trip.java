package com.hduser.parquet.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Trip {
	
	public Trip() {
		Conf.loadTable("APPROVAL_TRIPCOSTS");
		Conf.loadTable("PERIODS");
	}
	
	public String sql_trip(int period) {
		return
			"\n" + 
			"select \n" + 
			"	EMPLOYEE_ID,\n" + 
			"	ALLOWANCE_ID,\n" + 
			"	sum(TOTAL),\n" + 
			"	TRIP_REQUEST_ID,\n" + 
			"	TRIP_LOCATION_ID,\n" + 
			"	LOCATION_ID,\n" + 
			"	PROJECT_ID,\n" + 
			"	PERIOD_ID\n" + 
			" from \n" + 
			"	(select START_DATE,END_DATE,PERIOD_ID\n" + 
			"	from 	PERIODS\n" + 
			"	where	PERIOD_ID = "+period+") p,\n" + 
			"	APROVAL_TRIPCOSTS m\n" + 
			" where \n" + 
			"	DATE between START_DATE and END_DATE\n" + 
			"	and IS_APROVAL =1\n" + 
			"	and COUNT_APPROVAL =3\n" + 
			" group by \n" + 
			"	EMPLOYEE_ID,\n" + 
			"	ALLOWANCE_ID,\n" + 
			"	TRIP_REQUEST_ID,\n" + 
			"	TRIP_LOCATION_ID,\n" + 
			"	LOCATION_ID,\n" + 
			"	PROJECT_ID,\n" + 
			"	PERIOD_ID";	
	}
	
	public Dataset<Row> getTrip(int period){
		Dataset<Row> trip = Conf.spark.sql(sql_trip(period));
		return trip;
	}

}
