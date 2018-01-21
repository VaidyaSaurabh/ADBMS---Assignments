package com.neu.edu.assignment6_part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "1st inner Join");
		job.setJarByClass(JoinReducer.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinMapper0.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinMapper1.class);
		job.getConfiguration().set("join.type", "inner");
		// job.setNumReduceTasks(0);
		job.setReducerClass(JoinReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[3]));
		
		FileSystem hdfs = FileSystem.get(conf);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if (hdfs.exists(new Path(args[3])))
				hdfs.delete(new Path(args[3]), true);


		boolean complete = job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Final Join");

		if (complete) {
			job2.setJarByClass(JoinReducer.class);

			MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, JoinMapper2.class);
			MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, JoinMapper3.class);
			job2.getConfiguration().set("join.type", "inner");
			job2.setReducerClass(JoinReducer.class);

			job2.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(job2, new Path(args[4]));

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			if (hdfs.exists(new Path(args[4])))
				hdfs.delete(new Path(args[4]), true);

			complete = job2.waitForCompletion(true);

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}

	}

}
