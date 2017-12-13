package com.hduser.parquet.timesheet;

import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.UnixTimestamp;

public class LateInEarlyOut {

	private final String HDFS = Conf.hdfsURL; 
	
	public LateInEarlyOut() {
		Conf.loadTable("PERIODS");
		Conf.loadTable("TA_EMPLOYEE_TIMESHEETS");
		Conf.loadTable("TA_WORKING_SHIFTS");
		Conf.loadTable("TA_WORKING_TYPES");
	}
	
	public String sql_late_in_early_out(int period) {
		return
				"	select \n" + 
				"			EMPLOYEE_ID,\n" + 
				"			PERIOD_ID,\n" + 
				"			WORKING_DATE,\n" + 
				"			(case \n" + 
				"				when TOTAL_WORKING > TOTAL_TIME\n" + 
				"				then \n" + 
				"					TOTAL_WORKING - TOTAL_TIME\n" + 
				"				else \n" + 
				"					0\n" + 
				"			end\n" + 
				"			) as LATE_IN_EARLY_OUT_TIME\n" + 
				"\n" + 
				"	from	\n" + 
				"		(\n" + 
				"		select\n" + 
				"			EMPLOYEE_ID,\n" + 
				"			PERIOD_ID,\n" + 
				"			WORKING_DATE,\n" + 
				"			cast(TIME_OUT as bigint) - cast(TIME_IN as bigint)  as TOTAL_WORKING,\n" + 
				"			cast(WORKING_END as bigint) - cast(WORKING_START as bigint)as TOTAL_TIME\n" + 
				"		from\n" + 
				"			TA_WORKING_SHIFTS as f\n" + 
				"		join \n" + 
				"			(\n" + 
				"			select\n" + 
				"				*\n" + 
				"			from\n" + 
				"				(select START_DATE,END_DATE,PERIOD_ID\n" + 
				"				from 	PERIODS\n" + 
				"				where	PERIOD_ID = "+period+") p,\n" + 
				"				TA_EMPLOYEE_TIMESHEETS s\n" + 
				"			where\n" + 
				"				s.APPROVED = 1\n" + 
				"				and APPROVED_WORKING_TYPE_ID IS NOT NULL\n" + 
				"				and s.WORKING_DATE between p.START_DATE and p.END_DATE\n" + 
				"			) as t\n" + 
				"			on f.WORKING_SHIFT_ID = t.APPROVED_SHIFT_ID\n" + 
				"		) as k\n" + 
				"	order by EMPLOYEE_ID";
	}
	
	public Dataset<Row> getLateInEarlyOut(int period){
		Dataset<Row> lateInEarlyOut = Conf.spark.sql(sql_late_in_early_out(period));
		lateInEarlyOut.write().mode(SaveMode.Overwrite).partitionBy("PERIOD_ID","EMPLOYEE_ID").parquet(this.HDFS+"data_depen");
		return lateInEarlyOut;
	}
	public static void main(String[] args) {
		Conf.loadTable("TA_EMPLOYEE_TIMESHEETS");
		Conf.spark.sql("select * from TA_EMPLOYEE_TIMESHEETS").show(10);
		Conf.spark.sql("select * from TA_EMPLOYEE_TIMESHEETS").printSchema();
		LateInEarlyOut latein = new LateInEarlyOut();
		Dataset<Row> data_latein = latein.getLateInEarlyOut(89);
		data_latein.write().mode(SaveMode.Overwrite)
		.partitionBy("PERIOD_ID","EMPLOYEE_ID").json(Conf.outURL+"data_latein");	

	}
}
