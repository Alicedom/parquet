package com.hduser.parquet.timesheet;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/*
 * Tinh income config max. 
 * 
 * Chua tinh duoc qua s'ang (-> Trung binh cong)
 * Can thiet ke s'ang = Income * Tong ngay di lam co Income ID/ Tong so ngay di lam
 * 
 * Ngay di lam = Period - Public holiday - TA working calendar - (T7, CN: Nghi co dinh)
 */
public class IncomeConfigs {
	public IncomeConfigs() {
		Conf.loadTable("PERIODS");
		Conf.loadTable("INCOME_CONFIGS");
	}
	
	public String sql_get_income(int period) {
		return
				"select * \n" + 
				"from\n" + 
				"	(select\n" + 
				"			INCOME_CONFIG_ID,\n" + 
				"			INCOME_ID,\n" + 
				"			EMPLOYEE_ID,\n" + 
				"			CUSTOM_VALUE\n" +
				"\n" + 
				"		from\n" + 
				"			(select START_DATE,END_DATE,PERIOD_ID\n" + 
				"			from 	PERIODS\n" + 
				"			where	PERIOD_ID = +"+period+") p,\n" + 
				"			INCOME_CONFIGS f\n" + 
				"\n" + 
				"		where\n" + 
				"			f.CUSTOM_VALUE IS NOT NULL\n" + 
				"			and f.START_DATE < p.END_DATE\n" + 
				"			and f.END_DATE > p.START_DATE \n" + 
				"		) table1\n" + 
				"join\n" + 
				"	(select\n" + 
				"			max(INCOME_CONFIG_ID) as MAX_ID\n" + 
				"		from\n" + 
				"			(select START_DATE,END_DATE,PERIOD_ID\n" + 
				"			from 	PERIODS\n" + 
				"			where	PERIOD_ID = "+period+") p,\n" + 
				"			INCOME_CONFIGS f\n" + 
				"\n" + 
				"		where\n" + 
				"			f.CUSTOM_VALUE IS NOT NULL\n" + 
				"			and f.START_DATE < p.END_DATE\n" + 
				"			and f.END_DATE > p.START_DATE \n" + 
				"		group by\n" + 
				"			EMPLOYEE_ID,\n" + 
				"			INCOME_ID\n" + 
				"\n" + 
				"		) table2\n" + 
				"on table1.INCOME_CONFIG_ID = table2.MAX_ID\n" + 
				"\n" + 
				"order by EMPLOYEE_ID, INCOME_ID";
	}
	
	public Dataset<Row> getIncome(int period) {
		return Conf.spark.sql(sql_get_income(period));
	}
	
	public Dataset<Row> getIncome(int period, int income) {
		return getIncome(period).filter(col("INCOME_ID").equalTo(income));
	}
	
	public static void main(String[] args) {
		IncomeConfigs income = new IncomeConfigs();
		income.getIncome(89, 46)
		.repartition(1)
		.write().mode(SaveMode.Overwrite).json(Conf.outURL+"income");
	}

}
