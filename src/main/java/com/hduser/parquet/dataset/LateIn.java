package com.hduser.parquet.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class LateIn {
	public LateIn() {
		Conf.loadTable("PERIODS");
		Conf.loadTable("TA_EMPLOYEE_LATE_IN_EARLY_OUT");
		Conf.loadTable("TA_EMPLOYEE_TIMESHEETS");
	}
	
	/*
	 * SQl tinh tong thoi gian den muon trong thang
	 */
	public String sql_lateIn(int period) {
		return
				"select\n" + 
				"	EMPLOYEE_ID,\n" + 
				"	PERIOD_ID,\n" + 
				"	sum(APPROVED_LATE_IN_HOURS*cast(APPROVED_LATE_IN as int)) /8 as LATE_IN,\n" + 
				"	sum(APPROVED_EARLY_OUT_HOURS*cast(APPROVED_EARLY_OUT as int)) /8 as EARLY_OUT\n" + 
				" from\n" + 
				"	(\n" + 
				"	select *\n" + 
				"	from\n" + 
				"		(select START_DATE,END_DATE,PERIOD_ID\n" + 
				"		from 	PERIODS\n" + 
				"		where	PERIOD_ID = "+period+") p,\n" + 
				"		TA_EMPLOYEE_TIMESHEETS s\n" + 
				"	where\n" + 
				"		s.APPROVED = 1\n" + 
				"		and s.COUNT_APPROVE = 3\n" + 
				"		and APPROVED_WORKING_TYPE_ID IS NOT NULL\n" + 
				"		and s.WORKING_DATE between p.START_DATE and p.END_DATE\n" + 
				"	) t\n" + 
				" join TA_EMPLOYEE_LATE_IN_EARLY_OUT o\n" + 
				" on t.EMPLOYEE_TIMESHEET_ID = o.EMPLOYEE_TIMESHEET_ID\n" + 
				" group by\n" + 
				"	EMPLOYEE_ID,\n" + 
				"	PERIOD_ID";
	}
	
	public Dataset<Row> getLateIn(int period){
		Dataset<Row> lateIn = Conf.spark.sql(sql_lateIn(period));
		return lateIn;
	}
	
	public static void main(String[] args) {
		long start, stop;
		
		start = System.currentTimeMillis();
		new LateIn().getLateIn(88).write().mode(SaveMode.Overwrite).json(Conf.outURL+"latein");;
		stop = System.currentTimeMillis();
		long time1 = stop - start;

		System.out.println(time1);
		start = System.currentTimeMillis();
		new LateIn().getLateIn(88).write().mode(SaveMode.Overwrite).json(Conf.outURL+"latein");;
		stop = System.currentTimeMillis();
		long time2 = stop - start;
		
		start = System.currentTimeMillis();
		new LateIn().getLateIn(89).write().mode(SaveMode.Overwrite).json(Conf.outURL+"latein");;
		stop = System.currentTimeMillis();
		long time3 = stop - start;
		
		start = System.currentTimeMillis();
		new LateIn().getLateIn(90).write().mode(SaveMode.Overwrite).json(Conf.outURL+"latein");;
		stop = System.currentTimeMillis();
		long time4 = stop - start;
		
		start = System.currentTimeMillis();
		new LateIn().getLateIn(91).write().mode(SaveMode.Overwrite).json(Conf.outURL+"latein");;
		stop = System.currentTimeMillis();
		long time5 = stop - start;		
		System.out.println(time1+"\n"+time2+"\n"+time3+"\n"+time4+"\n"+time5);
	}
}
