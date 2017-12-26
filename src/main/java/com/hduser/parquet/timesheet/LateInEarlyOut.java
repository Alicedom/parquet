package com.hduser.parquet.timesheet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class LateInEarlyOut {

	public LateInEarlyOut() {
		Conf.loadTable("PERIODS");
		Conf.loadTable("TA_EMPLOYEE_TIMESHEETS");
		Conf.loadTable("TA_WORKING_SHIFTS");
		Conf.loadTable("TA_WORKING_TYPES");
	}
/*
 * He thong LIEO khong co phe duyet truoc, phai tu tinh toan bang code
 * 
 * Cho phep lam bu thoi gian den muon hoac ve som
 * Tong LIEO = Tong thoi gian ca - Tong thoi gian di lam thuc te (Tong LIEO > 0)
 * Thoi gian ca = Bat dau ca -> Ket thuc ca
 * Thoi gian thuc = Time in -> Time out 
 * 	
 */
	public static String sql_late_in_early_out(int period) {
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
	public static String sqlRDBMS(int period) {
		return 
				"select \n" + 
				"		EMPLOYEE_ID,\n" + 
				"		PERIOD_ID,\n" + 
				"		WORKING_DATE,\n" + 
				"		(case \n" + 
				"			when TOTAL_WORKING > TOTAL_TIME\n" + 
				"			then \n" + 
				"				cast(TOTAL_WORKING - TOTAL_TIME as time)\n" + 
				"			else \n" + 
				"				cast('00:00:00' as time)\n" + 
				"		end\n" + 
				"		) as LATE_IN_EARLY_OUT_TIME\n" + 
				"\n" + 
				"from	\n" + 
				"	(\n" + 
				"	select\n" + 
				"		EMPLOYEE_ID,\n" + 
				"		PERIOD_ID,\n" + 
				"		WORKING_DATE,\n" + 
				"		TIME_OUT - TIME_IN as TOTAL_WORKING,\n" + 
				"		cast(WORKING_END as datetime) - cast(WORKING_START as datetime) as TOTAL_TIME\n" + 
				"	from\n" + 
				"		TA_WORKING_SHIFTS as f\n" + 
				"	join \n" + 
				"		(\n" + 
				"		select\n" + 
				"			EMPLOYEE_ID,\n" + 
				"			TIME_IN,\n" + 
				"			TIME_OUT,\n" + 
				"			APPROVED_SHIFT_ID,\n" + 
				"			APPROVED_WORKING_TYPE_ID,\n" + 
				"			WORKING_DATE,\n" + 
				"			PERIOD_ID\n" + 
				"		from\n" + 
				"			(select START_DATE,END_DATE,PERIOD_ID\n" + 
				"			from 	PERIODS\n" + 
				"			where	PERIOD_ID = "+period+") p,\n" + 
				"			TA_EMPLOYEE_TIMESHEETS s\n" + 
				"		where\n" + 
				"			s.APPROVED = 1\n" + 
				"			and APPROVED_WORKING_TYPE_ID IS NOT NULL\n" + 
				"			and s.WORKING_DATE between p.START_DATE and p.END_DATE\n" + 
				"		) as t\n" + 
				"		on f.WORKING_SHIFT_ID = t.APPROVED_SHIFT_ID\n" + 
				"	) as k\n" + 
				"order by EMPLOYEE_ID";
	}
	public Dataset<Row> getLateInEarlyOut(int period){
		Dataset<Row> lateInEarlyOut = Conf.spark.sql(sql_late_in_early_out(period));
//		lateInEarlyOut.write().mode(SaveMode.Overwrite).partitionBy("EMPLOYEE_ID").parquet(this.HDFS+"lateInEarlyOut/PERIOD_ID="+period);
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
