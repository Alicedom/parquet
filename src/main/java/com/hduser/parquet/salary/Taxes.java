package com.hduser.parquet.salary;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.hduser.parquet.timesheet.Conf;

public class Taxes {

	public Dataset<Row> getSalaryTotalTaxStat(int period){
		Dataset<Row> totalSalary = Conf.spark.read().json(Conf.outURL+"getTotalSalary");
		System.out.println(totalSalary.schema().catalogString());
		totalSalary.printSchema();
		totalSalary.createOrReplaceTempView("TOTAL");

		Conf.spark.sql("Select * from TOTAL").limit(10).show();
		
		String Sql = "select \n" + 
				"	* ,\n" + 
				"	case\n" + 
				"		when TOTAL_SALARY < 05*1000000 then 0.05 * TOTAL_SALARY\n" + 
				"		when TOTAL_SALARY < 10*1000000 then 0.10 * TOTAL_SALARY - 0.25*1000000\n" + 
				"		when TOTAL_SALARY < 18*1000000 then 0.15 * TOTAL_SALARY - 0.75*1000000\n" + 
				"		when TOTAL_SALARY < 32*1000000 then 0.20 * TOTAL_SALARY - 1.65*1000000\n" + 
				"		when TOTAL_SALARY < 52*1000000 then 0.25 * TOTAL_SALARY - 3.25*1000000\n" + 
				"		when TOTAL_SALARY < 80*1000000 then 0.30 * TOTAL_SALARY - 5.85*1000000\n" + 
				"		else				   				0.35 * TOTAL_SALARY - 9.85*1000000\n" + 
				"	end as TAX_STAT\n" + 
				" from\n" + 
				"		TOTAL";
		
		

		Dataset<Row> salaryTotalTaxStat= Conf.spark.sql(Sql);
//		Dataset<Row> salaryTotalTaxStat= totalSalary.withColumn("TAX_STAT", col("TOTAL_SALARY").);
		
		salaryTotalTaxStat.orderBy("EMPLOYEE_ID").repartition(4)
		.write().mode(SaveMode.Overwrite).parquet(Conf.hdfsURL + "salaryTotalTaxStat");
		return salaryTotalTaxStat;
	}
	
	public static void main(String[] args) {

		//		Dataset<Row> getTotalSalary = new Salary().getTotalSalary(89);
		//		getTotalSalary.orderBy("EMPLOYEE_ID").repartition(1).write().mode(SaveMode.Overwrite).json(Conf.outURL+"getTotalSalary");
		Dataset<Row> getSalaryTotalTaxStat= new Taxes().getSalaryTotalTaxStat(89);
		getSalaryTotalTaxStat.orderBy("EMPLOYEE_ID").repartition(1).write().mode(SaveMode.Overwrite).json(Conf.outURL+"getSalaryTotalTaxStat");	
	}
}
