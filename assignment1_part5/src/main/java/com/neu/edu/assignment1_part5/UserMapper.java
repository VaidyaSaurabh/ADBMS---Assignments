package com.neu.edu.assignment1_part5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("::");
		IntWritable t1 = new IntWritable(new Integer(fields[0]));
		context.write(t1, one);
	}

}
