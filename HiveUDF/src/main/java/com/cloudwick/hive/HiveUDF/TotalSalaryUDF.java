package com.cloudwick.hive.HiveUDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class TotalSalaryUDF extends UDF {
	public Integer evaluate(Integer salary1, Integer salary2) {
		int n = salary1.intValue();
		int m = salary2.intValue();

		return new Integer(n + m);
	}
}