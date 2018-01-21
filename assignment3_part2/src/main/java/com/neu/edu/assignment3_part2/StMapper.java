package com.neu.edu.assignment3_part2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StMapper extends Mapper<Object, Text, IntWritable, StWritable> {

	private IntWritable Year = new IntWritable();
	private StWritable StWritable = new StWritable();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer a = new StringTokenizer(value.toString());

		if (value.toString().length() > 80) {
			return;
		} else {
			while (a.hasMoreTokens()) {

				SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd");

				try {
					String[] arr = a.nextToken().split(",");

					String stdate = arr[2];

					Date creationDate = (Date) format.parse(stdate);

					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");

					Year.set(Integer.parseInt((dateFormat.format(creationDate))));

					StWritable.setAverage(Double.parseDouble(arr[8]));

					StWritable.setCount(1);

					context.write(Year, StWritable);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}
}
