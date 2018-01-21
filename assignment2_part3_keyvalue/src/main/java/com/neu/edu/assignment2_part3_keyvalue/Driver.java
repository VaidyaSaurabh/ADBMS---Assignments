package com.neu.edu.assignment2_part3_keyvalue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		// Create configuration
		Configuration conf = new Configuration(true);

		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");

		// Create job
		Job job = new Job(conf, "KeyValue");
		job.setJarByClass(KeyValueMapper.class);

		// Setup MapReduce
		job.setMapperClass(KeyValueMapper.class);
		job.setReducerClass(KeyValueReducer.class);
		job.setNumReduceTasks(1);


		// Input

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		KeyValueTextInputFormat.addInputPath(job, inputPath);
		
		// Specify key / value

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);

	}
}