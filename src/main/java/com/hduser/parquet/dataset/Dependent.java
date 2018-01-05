package com.hduser.parquet.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class Dependent {

	public Dependent() {
		Conf.loadTable("EMPLOYEE_RELATIVES");
		Conf.loadTable("EMPLOYEE_DEPENDANTS");
		Conf.loadTable("PERIODS");
	}

	/*
	 * SQl tinh do nguoi phu thuoc va tong tien giam tru phu thuoc
	 */
	public String sql_dependent(int period) {
		return 
				"select \n" + 
				"	EMPLOYEE_ID,\n" + 
				"	count(t.EMPLOYEE_DEPENDANT_ID) as DEPENDENT_NUMBER,\n" + 
				"	count(t.EMPLOYEE_DEPENDANT_ID) * 3.6 * 1000000 +9*1000000 as DEPENDENT_SALARY,\n" + 
				"	PERIOD_ID\n" + 
				" from\n" + 
				"	(\n" + 
				"		select EMPLOYEE_RELATIVE_ID, EMPLOYEE_DEPENDANT_ID, PERIOD_ID\n" + 
				"		from \n" + 
				"			(select START_DATE,END_DATE,PERIOD_ID\n" + 
				"			from 	PERIODS\n" + 
				"			where	PERIOD_ID = "+ period+") p,\n" + 
				"			EMPLOYEE_DEPENDANTS t\n" + 
				"		where \n" + 
				"			t.START_DATE < p.END_DATE\n" + 
				"			and t.END_DATE > p.START_DATE \n" + 
				"		) t\n" + 
				" join EMPLOYEE_RELATIVES v\n" + 
				" on t.EMPLOYEE_RELATIVE_ID = v.EMPLOYEE_RELATIVE_ID\n" + 
				" group by EMPLOYEE_ID, PERIOD_ID";
	}
	
	public Dataset<Row> getDependent(int period){
		Dataset<Row> dependent = Conf.spark.sql(sql_dependent(period));
		return dependent;
	}
	
	public static void main(String[] args) {
		Dependent dependent = new Dependent();
		Dataset<Row> data_depen = dependent.getDependent(89);
		data_depen.write().mode(SaveMode.Overwrite).partitionBy("PERIOD_ID").json(Conf.outURL+"dependent");
	}

}
