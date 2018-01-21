package com.neu.edu.assignment5_part2;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupMonthPartitioner extends Partitioner<IntWritable, Text> implements Configurable {

	private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.month";
	private Configuration conf = null;
	private int minLastAccessDateYear = 0;

	public int getPartition(IntWritable key, Text value, int numPartitions) {
		return key.get() - minLastAccessDateYear;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
		minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
	}

	public static void setMinLastAccessDate(Job job, int month) {
		job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR, month);
	}
}