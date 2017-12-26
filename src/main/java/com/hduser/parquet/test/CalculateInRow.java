package com.hduser.parquet.test;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import com.hduser.parquet.timesheet.Timesheets;

import scala.Tuple2;

public class CalculateInRow {
	public CalculateInRow() {
		Dataset<Row> basicSalary = new Timesheets().getTimesheet(89);
		JavaPairRDD<Row, Integer> anasylisIPRow = basicSalary.toJavaRDD().mapToPair(r->{
			int os_code = r.getInt(4);
			boolean ispc = true;
			if(os_code < 8)
				ispc = false;
			Row newRow = RowFactory.create(r.getLong(0), r.getLong(2), ispc);
			return new Tuple2<Row, Integer>(newRow,1);
		});
		
		anasylisIPRow.cache();
	}

}
