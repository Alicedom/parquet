package com.hduser.parquet.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class OverTimes {

	public OverTimes() {
		Conf.loadTable("PERIODS");
		Conf.loadTable("TA_EMPLOYEE_TIMESHEETS");
		Conf.loadTable("TA_EMPLOYEE_OVERTIMES");
		Conf.loadTable("TA_OVERTIME_SETTING");
	}
	
	public String sql_overtime(int period) {
		return 
				"select\n" + 
				"		EMPLOYEE_ID,\n" + 
				"		PERIOD_ID,\n" + 
				"		OVERTIME_NAME,\n" + 
				"		sum(APPROVED_HOURS) as SUM_HOURS,\n" + 
				"		sum(APPROVED_HOURS) * cast(SUBSTRING(OVERTIME_NAME,4,3) as int) / 100 as CO_OVERTIME,\n" +
				"		sum(APPROVED_HOURS) * (cast(SUBSTRING(OVERTIME_NAME,4,3) as int) - 100) / 100 as CO_OVERTIME_NO_TAX\n" +
				" from	\n" + 
				"	(select\n" + 
				"		EMPLOYEE_ID,\n" + 
				"		PERIOD_ID,\n" + 
				"		OT_INCOME_ID,\n" + 
				"		APPROVED_HOURS,\n" + 
				"		APPROVED\n" + 
				"	from\n" + 
				"		TA_EMPLOYEE_OVERTIMES t\n" + 
				"	join	\n" + 
				"		(select\n" + 
				"			EMPLOYEE_ID,\n" + 
				"			EMPLOYEE_TIMESHEET_ID,\n" + 
				"			PERIOD_ID\n" + 
				"		from\n" + 
				"			(select START_DATE,END_DATE,PERIOD_ID\n" + 
				"			from 	PERIODS\n" + 
				"			where	PERIOD_ID = "+period+") p,\n" + 
				"			TA_EMPLOYEE_TIMESHEETS s\n" + 
				"\n" + 
				"		where\n" + 
				"			APPROVED_WORKING_TYPE_ID IS NOT NULL\n" + 
				"			and s.WORKING_DATE between p.START_DATE and p.END_DATE\n" + 
				"		) s\n" + 
				"	on\n" + 
				"		s.EMPLOYEE_TIMESHEET_ID = t.EMPLOYEE_TIMESHEET_ID\n" + 
				"	where \n" + 
				"		APPROVED = 1\n" + 
				"	) ot\n" + 
				" join\n" + 
				"	TA_OVERTIME_SETTING st\n" + 
				" on \n" + 
				"	st.OVERTIME_SETTING_ID = ot.OT_INCOME_ID\n" + 
				" group by \n" + 
				"		EMPLOYEE_ID,\n" + 
				"		PERIOD_ID,\n" + 
				"		OT_INCOME_ID,\n" + 
				"		OVERTIME_NAME";
	}
	
	
	
	public Dataset<Row> getOvertime(int period){
		Dataset<Row> overtime = Conf.spark.sql(sql_overtime(period));
		return overtime;
	}
	
	public static void main(String[] args) {
		int period = 89;
		Dataset<Row> overtime = new OverTimes().getOvertime(period);
		overtime.orderBy("EMMPLOYEE_ID").write().mode(SaveMode.Overwrite).json(Conf.outURL+"overtime/PERIOD_ID="+period);
	}
}
