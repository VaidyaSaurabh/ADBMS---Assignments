package com.neu.edu.assignment6_part4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MTitleMapper extends Mapper<Object, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] itr = value.toString().split(",");
		outputKey.set(itr[0]);
		outputValue.set("M" + value.toString());
		context.write(outputKey, outputValue);

	}
}