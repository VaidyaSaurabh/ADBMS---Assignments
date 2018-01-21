package com.neu.edu.assignment2_part3_keyvalue;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class KeyValueMapper extends Mapper<Text, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		context.write(key, new IntWritable(Integer.parseInt(value.toString())));

	}
}