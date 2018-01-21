package com.neu.edu.assignment5_part2;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartitionMapper extends Mapper<Object, Text, IntWritable, Text> {

	private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	private IntWritable outkey = new IntWritable();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] line = value.toString().split(" ");
		String timeStamp = line[3];
		String date = timeStamp.substring(1, timeStamp.length()).trim();
		String[] array = date.toString().split("/");

		String month = array[1];

		if (month.trim().equals("Jan")) {
			outkey.set(1);
		} else if (month.trim().equals("Feb")) {
			outkey.set(2);
		} else if (month.trim().equals("Mar")) {
			outkey.set(3);
		} else if (month.trim().equals("Apr")) {
			outkey.set(4);
		} else if (month.trim().equals("May")) {
			outkey.set(5);
		} else if (month.trim().equals("Jun")) {
			outkey.set(6);
		} else if (month.trim().equals("Jul")) {
			outkey.set(7);
		} else if (month.trim().equals("Aug")) {
			outkey.set(8);
		} else if (month.trim().equals("Sep")) {
			outkey.set(9);
		} else if (month.trim().equals("Oct")) {
			outkey.set(10);
		} else if (month.trim().equals("Nov")) {
			outkey.set(11);
		} else if (month.trim().equals("Dec")) {
			outkey.set(12);
		}

		context.write(outkey, value);
	}
}
