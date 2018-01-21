package com.neu.edu.assignment2_part4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StMapper extends Mapper<Object, Text, Text, CustomWritable> {

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String values[] = value.toString().split(",");
		if (values.length < 8)
			return;
		try {
			Text t1 = new Text(values[1]);
			CustomWritable stuff = new CustomWritable (values[2], Long.parseLong(values[7]),
					Float.parseFloat(values[8]));

			context.write(t1, stuff);
		} catch (Exception e) {
			return;
		}

	}

}
