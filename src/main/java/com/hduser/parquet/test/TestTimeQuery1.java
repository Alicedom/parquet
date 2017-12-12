package com.hduser.parquet.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hduser.parquet.timesheet.Conf;

public class TestTimeQuery1 {

	public TestTimeQuery1() {
		Conf.loadTable("PERIODS");
		Conf.loadTable("TA_EMPLOYEE_TIMESHEETS");
		Conf.loadTable("TA_WORKING_SHIFTS");
		Conf.loadTable("TA_WORKING_TYPES");
	}

	public String sql_timesheet(int period){
		String sql_timesheet=
				"select\n" + 
						"	COEFFICIENT,\n" + 
						"	IS_NIGHT_SHIFT,\n" + 
						"	EMPLOYEE_ID,\n" + 
						"	WORKING_TYPE_NAME,\n" + 
						"	NUMBER_DATE,\n" + 
						"	PERIOD_ID\n" + 
						"from \n" + 
						"	TA_WORKING_TYPES t\n" + 
						"join 	(\n" + 
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
						"on e.APPROVED_WORKING_TYPE_ID = t.WORKING_TYPE_ID\n" + 
						"order by EMPLOYEE_ID	";

		return sql_timesheet;
	}

	public Dataset<Row> getTimesheet(int period){
		return Conf.spark.sql(sql_timesheet(period));
	}
	public void getRDBMS(int period) {

		Connection con;
		try {
			con = DriverManager.getConnection(
					"jdbc:sqlserver://localhost;databaseName=eHRM_Hamaden",
					"sa",
					"Khanhno1");


			Statement stmt = con.createStatement();
			stmt.executeQuery(sql_timesheet(period));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public static void main(String[] args) {
		long start, stop;
		TestTimeQuery1 test;

		start = System.currentTimeMillis();
		test = new TestTimeQuery1();
		test.getTimesheet(89);
		stop = System.currentTimeMillis();
		long time1= stop -start;

		start = System.currentTimeMillis();
		test.getRDBMS(89);
		stop = System.currentTimeMillis();		
		long time2 = stop-start;
		

		start = System.currentTimeMillis();
		Conf.spark.newSession();
		test = new TestTimeQuery1();
		test.getTimesheet(89);
		stop = System.currentTimeMillis();
		long time3= stop -start;

		start = System.currentTimeMillis();
		test.getRDBMS(89);
		stop = System.currentTimeMillis();		
		long time4 = stop-start;
		
		
		start = System.currentTimeMillis();
		Conf.spark.newSession();
		test = new TestTimeQuery1();
		test.getTimesheet(89);
		stop = System.currentTimeMillis();
		long time5= stop -start;

		start = System.currentTimeMillis();
		test.getRDBMS(89);
		stop = System.currentTimeMillis();		
		long time6 = stop-start;
		
		
		start = System.currentTimeMillis();
		Conf.spark.newSession();
		test = new TestTimeQuery1();
		test.getTimesheet(89);
		stop = System.currentTimeMillis();
		long time7= stop -start;

		start = System.currentTimeMillis();
		test.getRDBMS(89);
		stop = System.currentTimeMillis();		
		long time8 = stop-start;
		
		System.out.println(time1+"\n"+time2+"\n"+time3+"\n"+time4+"\n"+time5+"\n"+time6+"\n"+time7+"\n"+time8+"\n");
	}


}
