package com.cloudwick.PigUDF;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class MyFilter extends FilterFunc {
	@Override
	public Boolean exec(Tuple tuple) throws IOException {
		Integer deptid = (Integer) tuple.get(2);
		if (deptid.intValue() > 15) {
			return true;
		}

		return false;
	}
}