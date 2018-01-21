package com.neu.edu.assignment1_part3;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageStockMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split(",");
		if (fields[0].contains("NYSE")) {
			Text t1 = new Text(fields[1]);
			FloatWritable stockPrice = new FloatWritable(new Float(fields[4]));
			context.write(t1, stockPrice);
		}

	}
}