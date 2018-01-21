package com.neu.edu.assignment4_part4;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FilteringMapper extends Mapper<Object, Text, NullWritable, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split(" ");

		String txt = new String(fields[fields.length-2]);

		if (txt.contains("404")) {
			context.write(NullWritable.get(), new Text (fields[0]));
		}

	}
}