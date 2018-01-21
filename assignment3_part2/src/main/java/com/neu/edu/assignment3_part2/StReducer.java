package com.neu.edu.assignment3_part2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

class StReducer extends Reducer<IntWritable, StWritable, IntWritable, StWritable> {

	private StWritable result = new StWritable();

	public void reduce(IntWritable key, Iterable<StWritable> values, Context context)
			throws IOException, InterruptedException {

		double sum = 0;
		long count = 0;

		for (StWritable value : values) {
			sum += value.getCount() * value.getAverage();
			count += value.getCount();
		}

		result.setCount(count);
		result.setAverage(sum / count);

		context.write(key, result);

	}
}
