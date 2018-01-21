package com.neu.edu.assignment2_part5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer2 extends Reducer<CustomWritableMovie, IntWritable, CustomWritableMovie, IntWritable> {

	public static int count = 1;

	@Override
	protected void reduce(CustomWritableMovie key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		for (IntWritable val : values) {

			if (count < 26) {
				context.write(key, new IntWritable(count));
				count++;
			} else {
				break;
			}
		}
	}
}
