package com.neu.edu.assignment6_part4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Hierarchy");
		job.setJarByClass(Driver.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MTitleMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TagMapper.class);

		job.setReducerClass(HierarchialReducer.class);
	
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		
	FileSystem hdfs = FileSystem.get(conf);
		
		if (hdfs.exists(new Path(args[2])))
			hdfs.delete(new Path(args[2]), true);

		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}
}