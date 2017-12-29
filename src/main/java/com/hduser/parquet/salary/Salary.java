package com.hduser.parquet.salary;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.hduser.parquet.dataset.Conf;
import com.hduser.parquet.dataset.Dependent;
import com.hduser.parquet.dataset.IncomeConfigs;
import com.hduser.parquet.dataset.OverTimes;
import com.hduser.parquet.dataset.Periods;
import com.hduser.parquet.dataset.Timesheets;

public class Salary {

	int basicSalaryId = Conf.BASIC_SALARY_ID;

	public Salary() {

	}

	public double getWorkingDate(int period) {
		
		Dataset<Row> WorkingDate = new  Periods().getTA_Working_Date();

		return WorkingDate.filter(col("PERIOD_ID").equalTo(period)).select("WORKING_DATE").first().getDouble(0);
	}

	public void getSalary(int period) {

	double workingDate = getWorkingDate(period);

		Dataset<Row> income = new IncomeConfigs().getIncome(period, basicSalaryId);

		////////////Salary
		Dataset<Row> timesheet = new Timesheets().getTimesheet(period);
		Dataset<Row> timesheetSalary = 
				timesheet
				.join(income, "EMPLOYEE_ID")
				.withColumn("TIMESHEET_SALARY", timesheet.col("CO_TIMESHEET").multiply(income.col("CUSTOM_VALUE")).divide(workingDate))
				;

		Dataset<Row> overtime = new OverTimes().getOvertime(period);
		Dataset<Row> overtimeSalary = 
				overtime
				.join(income, "EMPLOYEE_ID")
				.withColumn("OVERTIME_SALARY", overtime.col("CO_OVERTIME").multiply(income.col("CUSTOM_VALUE")).divide(workingDate*8))
				;

		Dataset<Row> totalSalary 
			= (timesheetSalary.groupBy("EMPLOYEE_ID").sum("TIMESHEET_SALARY"))
			.join(overtimeSalary.groupBy("EMPLOYEE_ID").sum("OVERTIME_SALARY"),"EMPLOYEE_ID")
			.withColumn("TOTAL_SALARY", col("sum(TIMESHEET_SALARY)").plus(col("sum(OVERTIME_SALARY)")));
		
		totalSalary.orderBy("EMPLOYEE_ID").repartition(4)
		.write().mode(SaveMode.Overwrite).json(Conf.outURL + "totalSalary");

		////////////Dependent
		Dataset<Row> timesheetNoTax = timesheet.filter(col("IS_NIGHT_SHIFT").equalTo(1));
		Dataset<Row> timesheetNoTaxSalary =
				timesheetNoTax
				.join(income, "EMPLOYEE_ID")
				.withColumn("TIMESHEET_SALARY_NOTAX", timesheet.col("CO_TIMESHEET").multiply(income.col("CUSTOM_VALUE")).divide(workingDate))
				;
		
		timesheetNoTaxSalary.orderBy("EMPLOYEE_ID").repartition(4)
		.write().mode(SaveMode.Overwrite).json(Conf.outURL + "timesheetNoTaxSalary");
		
		Dataset<Row> overtimeNoTaxSalary =
				overtime
				.join(income, "EMPLOYEE_ID")
				.withColumn("OVERTIME_SALARY_NOTAX", overtime.col("CO_OVERTIME_NO_TAX").multiply(income.col("CUSTOM_VALUE")).divide(workingDate*8))
				;

		overtimeNoTaxSalary.orderBy("EMPLOYEE_ID").repartition(4)
		.write().mode(SaveMode.Overwrite).json(Conf.outURL + "overtimeNoTaxSalary");
		Dataset<Row> dependentperiod = new Dependent().getDependent(period);
		
		Conf.loadTable("EMPLOYEES");
		Dataset<Row> employee = Conf.spark.sql("SELECT * FROM EMPLOYEES");
	
//		Dataset<Row> totalSalary = Conf.spark.read().json(Conf.outURL + "totalSalary");
//		Dataset<Row> timesheetNoTaxSalary= Conf.spark.read().json(Conf.outURL + "timesheetNoTaxSalary");
//		Dataset<Row> overtimeNoTaxSalary = Conf.spark.read().json(Conf.outURL + "overtimeNoTaxSalary");
//		Dataset<Row> dependentperiod = Conf.spark.read().json(Conf.outURL + "dependent");
//	
				
		
		Dataset<Row> giamtru = 	
				employee
				.join(timesheetNoTaxSalary.groupBy("EMPLOYEE_ID").sum("TIMESHEET_SALARY_NOTAX"),
						employee.col("EMPLOYEE_ID").equalTo(timesheetNoTaxSalary.col("EMPLOYEE_ID")),"fullouter")
				.drop(timesheetNoTaxSalary.col("EMPLOYEE_ID"))
				.join(overtimeNoTaxSalary.groupBy("EMPLOYEE_ID").sum("OVERTIME_SALARY_NOTAX"),
						employee.col("EMPLOYEE_ID").equalTo(overtimeNoTaxSalary.col("EMPLOYEE_ID")),"fullouter")
				.drop(overtimeNoTaxSalary.col("EMPLOYEE_ID"))
				.join(dependentperiod,
						employee.col("EMPLOYEE_ID").equalTo(dependentperiod.col("EMPLOYEE_ID")), "fullouter")
				.drop(dependentperiod.col("EMPLOYEE_ID"))
				.withColumn("TOTAL_DEPENDENT", col("sum(TIMESHEET_SALARY_NOTAX)").plus(col("sum(OVERTIME_SALARY_NOTAX)").plus(col("DEPENDENT_SALARY")).plus(9*1000000)))
				;
					
		
		giamtru.orderBy("EMPLOYEE_ID").repartition(4)
		.write().mode(SaveMode.Overwrite).json(Conf.outURL + "giamtru");
		
		/////Salary before tax
		Dataset<Row> salaryBeforeTax =
				totalSalary.join(giamtru,"EMPLOYEE_ID")
				.withColumn("BEFORE_TAX_SALARY", col("TOTAL_SALARY").minus("TOTAL_DEPENDENT"));
		
		
		//Tax Stat & Net salary
		salaryBeforeTax.createOrReplaceTempView("TOTAL_SALARY");
		String Sql = "select\n" + 
				"	*,\n" + 
				"	TOTAL_SALARY - TAX_STAT as NET_SALARY\n" + 
				"\n" + 
				" from(	\n" + 
				"	select \n" + 
				"		* ,\n" + 
				"		case\n" + 
				"			when BEFORE_TAX_SALARY < 05*1000000 then 0.05 * BEFORE_TAX_SALARY*1000000\n" + 
				"			when BEFORE_TAX_SALARY < 10*1000000 then 0.10 * BEFORE_TAX_SALARY - 0.25*1000000\n" + 
				"			when BEFORE_TAX_SALARY < 18*1000000 then 0.15 * BEFORE_TAX_SALARY - 0.75*1000000\n" + 
				"			when BEFORE_TAX_SALARY < 32*1000000 then 0.20 * BEFORE_TAX_SALARY - 1.65*1000000\n" + 
				"			when BEFORE_TAX_SALARY < 52*1000000 then 0.25 * BEFORE_TAX_SALARY - 3.25*1000000\n" + 
				"			when BEFORE_TAX_SALARY < 80*1000000 then 0.30 * BEFORE_TAX_SALARY - 5.85*1000000\n" + 
				"			else				   0.35 * TOTAL_SALARY - 9.85*1000000\n" + 
				"		end as TAX_STAT\n" + 
				"	from\n" + 
				"			TOTAL_SALARY\n" + 
				")";
		
		Dataset<Row> salaryTotalTaxStat= Conf.spark.sql(Sql);
		salaryTotalTaxStat.orderBy("EMPLOYEE_ID").repartition(1)
		.write().mode(SaveMode.Overwrite).json(Conf.outURL + "netSalary/PERIOD_ID="+period);
		
	}
	public static void main(String[] args) {
		long start,stop;
		long fist = System.currentTimeMillis();
		start = System.currentTimeMillis();
		new Salary().getSalary(90);
		stop = System.currentTimeMillis();
		long time1 = stop -start;
		
		start = System.currentTimeMillis();
		new Salary().getSalary(88);
		stop = System.currentTimeMillis();
		long time2 = stop -start;
		
		start = System.currentTimeMillis();
		new Salary().getSalary(89);
		stop = System.currentTimeMillis();
		long time3 = stop -start;
		
		start = System.currentTimeMillis();
		new Salary().getSalary(90);
		stop = System.currentTimeMillis();
		long time4 = stop -start;
		
		start = System.currentTimeMillis();
		new Salary().getSalary(91);
		stop = System.currentTimeMillis();
		long time5 = stop -start;
//		
		long last = System.currentTimeMillis();
		long time = last - fist;
		System.out.println(time1+"\n"+time2+"\n"+time3+"\n"+time4+"\n"+time5+"\n"+time);
		
//		System.out.println(time1 +"\n"+time2);
	}




}
