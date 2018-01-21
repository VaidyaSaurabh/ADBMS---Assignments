package com.neu.edu.assignment4_part2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IpCounterMapper extends

		Mapper<Object, Text, NullWritable, NullWritable> {

	public static final String IP_COUNTER_GROUP = "IPs";

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(" ");
		Text textInput = new Text(fields[0]);
		if (textInput != null && !textInput.toString().isEmpty()) {
			context.getCounter(IP_COUNTER_GROUP, textInput.toString()).increment(1);
		}

	}

}
