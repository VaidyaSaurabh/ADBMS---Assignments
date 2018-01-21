package com.neu.edu.assignment4_part3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InvertedMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split(",");

		Float stPrice = 0.0f;
		String stName = "";
		String range = "";
		try {

			stName = values[1];
			stPrice = Float.parseFloat(values[3]);

		} catch (Exception e) {
		}
		try {

			Integer tmp = 0;
			tmp = (int) (stPrice / 10);
			range = (tmp * 10 + "-" + (tmp + 1) * 10);
			context.write(new Text(range), new Text(stName));
		} catch (Exception e) {

			System.out.println("" + e.getMessage());

		}

	}

}
