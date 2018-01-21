package com.neu.edu.assignment6_part2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper2 extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] separatedInput = value.toString().split(";");

		String userId = separatedInput[0];
		if (userId == null) {
			return;
		}
		outkey.set(userId);
		outvalue.set("A" + value);
		context.write(outkey, outvalue);
	}
}