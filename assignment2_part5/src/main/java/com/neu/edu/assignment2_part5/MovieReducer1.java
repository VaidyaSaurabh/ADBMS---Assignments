package com.neu.edu.assignment2_part5;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer1 extends Reducer<LongWritable, FloatWritable, LongWritable, FloatWritable> {

	@Override
	protected void reduce(LongWritable key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		FloatWritable result = new FloatWritable();
		Float sum = 0.F;
		long count = 0;
		Float avg = 0.F;
		for (FloatWritable val : values) {
			count++;
			sum += val.get();
		}
		avg = (Float) sum / count;
		result.set(avg);
		context.write(key, result);
	}
}
