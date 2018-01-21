package com.neu.edu.assignment2_part3_Sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new Driver(), args);

		System.exit(exitCode);

	}

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		
		}
		
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		
		Configuration conf = new Configuration(true);
		Job job = new Job(conf, "SeqFile");
		job.setJarByClass(SqFileMapper.class);

		job.setMapperClass(SqFileMapper.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// Delete output if exists
				FileSystem hdfs = FileSystem.get(conf);
				if (hdfs.exists(outputDir))
					hdfs.delete(outputDir, true);

				// Execute job
				int code = job.waitForCompletion(true) ? 0 : 1;
				System.exit(code);
	
		return code;
	
	}
}