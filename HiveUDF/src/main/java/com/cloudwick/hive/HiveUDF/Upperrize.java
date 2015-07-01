package com.cloudwick.hive.HiveUDF;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Upperrize extends UDF {
	public Text evaluate(Text name) {
		if (name == null) {
			return null;
		}

		String result = name.toString().toUpperCase();
		Text retResult = new Text(result);
		System.out.println(retResult.toString());
		return retResult;
	}
}