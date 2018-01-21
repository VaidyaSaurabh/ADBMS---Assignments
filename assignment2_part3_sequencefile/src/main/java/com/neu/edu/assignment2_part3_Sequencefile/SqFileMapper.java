package com.neu.edu.assignment2_part3_Sequencefile;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SqFileMapper extends Mapper {

	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}