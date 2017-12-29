package com.hduser.parquet.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import com.hduser.parquet.dataset.OverTimes;
import com.hduser.parquet.dataset.Timesheets;

public class TestTimeQuery1 {

	public long getRDBMS(int period) {
		long start = System.currentTimeMillis();		
		Connection con;
		try {
			con = DriverManager.getConnection(
					"jdbc:sqlserver://localhost;databaseName=eHRM_Hamaden",
					"sa",
					"Khanhno1");
			Statement stmt = con.createStatement();
			stmt.executeQuery(new Timesheets().sql_timesheet(period));
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

		long stop = System.currentTimeMillis();
		return stop - start;

	}

	public long getHDFS( int period) {
		long start = System.currentTimeMillis();

//		Timesheets t = new Timesheets();
//		t.getTimesheet(period);

//		LateInEarlyOut LIEO = new LateInEarlyOut();
//		LIEO.getLateInEarlyOut(period);
		
		OverTimes over = new OverTimes();
		over.getOvertime(period);
		long stop = System.currentTimeMillis();

		return stop - start;
	}
	public static void main(String[] args) {
		TestTimeQuery1 test = new TestTimeQuery1();
		LinkedList<Long> time = new LinkedList<Long>();
		time.add(test.getRDBMS(88));
		time.add(test.getHDFS(88));
		time.add(test.getRDBMS(88));
		time.add(test.getHDFS(88));
		time.add(test.getRDBMS(89));
		time.add(test.getHDFS(89));
		time.add(test.getRDBMS(90));
		time.add(test.getHDFS(90));
		time.add(test.getRDBMS(91));
		time.add(test.getHDFS(91));
		
		System.out.println(time.toString());
	}
}
