package com.cloudwick.PigUDF2;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class MyFilter extends EvalFunc<String> {
	@Override
	public String exec(Tuple input) throws IOException {
		return input.get(0).toString().toUpperCase();
	}
}