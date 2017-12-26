package com.hduser.parquet.timesheet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

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
	public Dataset<Row> getTA_Working_Date() {
		Dataset<Row> workingDate = Conf.spark.sql(sql_working_date);
		workingDate.repartition(4).write().mode(SaveMode.Overwrite).parquet(Conf.hdfsURL+"TA_WORKING_CALENDAR");
		return workingDate;
	}
	

	public static void main(String[] args) {
		Periods period = new Periods();
		System.out.println(period.getStartDate(89));
		System.out.println(period.getEndDate(89));
		Conf.spark.close();
	}

}
