package com.hduser.parquet.timesheet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class Timesheets {
	public Timesheets() {
		Conf.loadTable("PERIODS");
		Conf.loadTable("TA_EMPLOYEE_TIMESHEETS");
		Conf.loadTable("TA_WORKING_SHIFTS");
		Conf.loadTable("TA_WORKING_TYPES");
	}

	public String sql_timesheet(int period){
		String sql_timesheet=
				"select\n" + 
						"	IS_NIGHT_SHIFT,\n" + 
						"	EMPLOYEE_ID,\n" + 
						"	COEFFICIENT,\n" + 
						"	NUMBER_DATE,\n" + 
						"	COEFFICIENT * NUMBER_DATE as BASIC_CO,\n" + 
						"	WORKING_TYPE_ID,\n" + 
						"	WORKING_TYPE_NAME,\n" + 
						"	PERIOD_ID" +
						" from \n" + 
						"	TA_WORKING_TYPES t\n" + 
						" join 	(\n" + 
						"	select\n" + 
						"		EMPLOYEE_ID,\n" + 
						"		APPROVED_WORKING_TYPE_ID,\n" + 
						"		PERIOD_ID,\n" + 
						"		count(s.EMPLOYEE_ID) as NUMBER_DATE\n" + 
						"\n" + 
						"	from\n" + 
						"		(select START_DATE,END_DATE,PERIOD_ID\n" + 
						"		from 	PERIODS\n" + 
						"		where	PERIOD_ID = "+period+") p,\n" + 
						"		TA_EMPLOYEE_TIMESHEETS s\n" + 
						"	where\n" + 
						"		s.APPROVED = 1\n" + 
						"		and APPROVED_WORKING_TYPE_ID IS NOT NULL\n" + 
						"		and s.WORKING_DATE between p.START_DATE and p.END_DATE\n" + 
						"\n" + 
						"	group by\n" + 
						"		EMPLOYEE_ID,\n" + 
						"		APPROVED_WORKING_TYPE_ID,\n" + 
						"		PERIOD_ID\n" + 
						"	) e\n" + 
						"\n" + 
						" on e.APPROVED_WORKING_TYPE_ID = t.WORKING_TYPE_ID\n" + 
						" order by EMPLOYEE_ID	"
						;

		return sql_timesheet;
	}

	public Dataset<Row> getTimesheet(int period){
		return Conf.spark.sql(sql_timesheet(period));
	}

	public static void main(String[] args) {
		new Timesheets().getTimesheet(89)
//		.withColumn("", col("NUMBER_DATE").multiply( col("COEFFICIENT")))
		.repartition(1)
		.write().mode(SaveMode.Overwrite).json(Conf.outURL+"timesheets1");
	}




}
