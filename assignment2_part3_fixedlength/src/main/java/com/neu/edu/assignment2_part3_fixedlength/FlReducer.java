package com.neu.edu.assignment2_part3_fixedlength;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FlReducer extends Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {

	@Override

	public void reduce(LongWritable key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {

		for (BytesWritable val : values) {

			context.write(key, val);
		}

	}

}