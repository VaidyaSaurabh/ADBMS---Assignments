package com.neu.edu.assignment4_part5;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilteringReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

	public void reduce(Text key, Iterable<NullWritable> value, Context context)
			throws IOException, InterruptedException {

		context.write(key, NullWritable.get());

	}
}