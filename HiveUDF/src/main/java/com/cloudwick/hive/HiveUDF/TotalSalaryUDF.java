package com.cloudwick.hive.HiveUDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class TotalSalaryUDF extends UDF {
	public Integer evaluate(Integer salary1, Integer salary2) {
		if (salary1 == null) {
			return salary2;
		} else if (salary2 == null) {
			return salary1;
		}

		int n = salary1.intValue();
		int m = salary2.intValue();

		return new Integer(n + m);
	}
}