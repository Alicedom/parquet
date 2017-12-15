package com.hduser.parquet.timesheet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class LateInSecond {
	
	public LateInSecond() {
		Conf.loadTable("PERIODS");
		Conf.loadTable("TA_EMPLOYEE_TIMESHEETS");
		Conf.loadTable("TA_WORKING_SHIFTS");
		Conf.loadTable("TA_WORKING_TYPES");
		
	}
/*
 * He thong LIEO khong co phe duyet truoc, phai tu tinh toan bang code
 * 
 * Khong cho phep bu thoi gian den muon va ve som
 * Tong LIEO = Tong LI + Tong EO
 * Tong LI = Bat cau ca - Time in ( LI >0)
 * Tong EO = Time Out - Ket thuc ca ( EO >0)
 */
	public String sql_lateIn(int period) {
		return
				"			select \n" + 
				"		EMPLOYEE_ID,\n" + 
				"		WORKING_START,\n" + 
				"		TIME_IN,\n" + 
				"		TIME_OUT,\n" + 
				"		WORKING_END,\n" + 
				"		WORKING_DATE,\n" + 
				"		PERIOD_ID,\n" + 
				"		cast(LATE_IN as timestamp) as LATE_IN,\n" + 
				"		cast(EARLY_OUT as timestamp) as EARLY_OUT,\n" + 
				"		cast(LATE_IN + EARLY_OUT as timestamp) as LIEO_TIME\n" + 
				"	from\n" + 
				"		(select\n" + 
				"			*,		\n" + 
				"			case \n" + 
				"				when cast(WORKING_DATE as int) + cast(f.WORKING_START as int) < cast(TIME_IN as int)\n" + 
				"				then - cast(WORKING_DATE as int) - cast(f.WORKING_START as int) + cast(TIME_IN as int) \n" + 
				"				else null\n" + 
				"			end\n" + 
				"			as LATE_IN,\n" + 
				"			case \n" + 
				"				when cast(WORKING_DATE as int) + cast(f.WORKING_END as int) > cast(TIME_OUT as int)\n" + 
				"				then cast(WORKING_DATE as int) + cast(f.WORKING_END as int) - cast(TIME_OUT as int) \n" + 
				"				else null\n" + 
				"			end\n" + 
				"			as EARLY_OUT\n" + 
				"		from\n" + 
				"			TA_WORKING_SHIFTS f\n" + 
				"		join \n" + 
				"			(\n" + 
				"			select\n" + 
				"				EMPLOYEE_ID,\n" + 
				"				TIME_IN,\n" + 
				"				TIME_OUT,\n" + 
				"				APPROVED_WORKING_TYPE_ID,\n" + 
				"				APPROVED_SHIFT_ID,\n" + 
				"				WORKING_DATE,\n" + 
				"				PERIOD_ID\n" + 
				"			from\n" + 
				"				(select START_DATE,END_DATE,PERIOD_ID\n" + 
				"				from 	PERIODS\n" + 
				"				where	PERIOD_ID = 89) p,\n" + 
				"				TA_EMPLOYEE_TIMESHEETS s\n" + 
				"			where\n" + 
				"				s.APPROVED = 1\n" + 
				"				and APPROVED_WORKING_TYPE_ID IS NOT NULL\n" + 
				"				and s.WORKING_DATE between p.START_DATE and p.END_DATE\n" + 
				"			) t\n" + 
				"		on f.WORKING_SHIFT_ID = t.APPROVED_SHIFT_ID\n" + 
				"		) m\n" + 
				"	order by EMPLOYEE_ID\n" + 
				"";
	}
	
	public Dataset<Row> getLateIn(int period){
		Dataset<Row> lateIn = Conf.spark.sql(sql_lateIn(period));
		return lateIn;
	}
	
	public static void main(String[] args) {
		Dataset<Row>data_late2 = new LateInSecond().getLateIn(89);
	
		data_late2.repartition(1).write().mode(SaveMode.Overwrite)
		.partitionBy("PERIOD_ID").json(Conf.outURL+"data_latein2v5");
		data_late2.show(10);

	}

}
