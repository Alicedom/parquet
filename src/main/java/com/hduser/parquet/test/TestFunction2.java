package com.hduser.parquet.test;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class TestFunction2 {
	//Reduce Function for cumulative sum
    Function2<String, Boolean, Integer> reduceSumFunc = new Function2<String, Boolean, Integer>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Integer call(String arg0, Boolean arg1) throws Exception {
			// TODO Auto-generated method stub
			return 1;
		}
	};
    
    //Reduce Function for cumulative multiplication
    Function2<Integer, Integer, Integer> reduceMulFunc = (accum, n) -> (accum * n);
    
    Function<Double	, Double> taxFun = new Function<Double, Double>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Double call(Double arg0) throws Exception {
			Double tax = 0.0;
			if		(arg0 < 05) tax = 0.05 * arg0;
			else if (arg0 < 10)	tax = 0.10 * arg0 - 0.25;
			else if (arg0 < 18) tax = 0.15 * arg0 - 0.75;
			else if (arg0 < 32) tax = 0.20 * arg0 - 1.65;
			else if (arg0 < 52) tax = 0.25 * arg0 - 3.25;
			else if (arg0 < 80) tax = 0.30 * arg0 - 5.85;
			else 				tax = 0.35 * arg0 - 9.85; 
			return tax;
		}
    	
	}; 
	
	Function<Integer, Boolean> filterPredicate = e -> e % 2 == 0;
	

}
