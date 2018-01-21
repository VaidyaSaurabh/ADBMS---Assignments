package com.neu.edu.assignment1_part5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class UserReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private IntWritable total = new IntWritable();

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		total.set(sum);
		context.write(key, total);
	}

}