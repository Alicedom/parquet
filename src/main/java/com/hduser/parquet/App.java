package com.hduser.parquet;

import com.hduser.parquet.salary.Salary;

/**
 * Hello world!
 * Class test with salary 89
 */
public class App 
{
    public static void main( String[] args )
    {
    	for (String string : args) {
    		new Salary().getSalary(Integer.valueOf(string));
		}
    	
        
    }
}
