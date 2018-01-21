package com.neu.edu.assignment1_part3;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageStockReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	private FloatWritable total = new FloatWritable();

	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		float sum = 0;
		int count = 0;
		for (FloatWritable val : values) {
			count++;
			sum += val.get();
		}
		total.set(sum / count);
		context.write(key, total);
	}

}