package com.neu.edu.assignment6_part3;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
	public static void main(String args[])
			throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

		Configuration conf = new Configuration();
		if (args.length != 5) {
			System.err.println("Insufficient <input> <output>");
			System.exit(2);
		}

		Path cacheFile1 = new Path(args[0]);
		Path cacheFile2 = new Path(args[1]);
		Path inputPath = new Path(args[2]);
		Path outputInter = new Path(args[3]);
		Path outputDir = new Path(args[4]);

		Job job = Job.getInstance(conf, "Map-side join");

		job.setJarByClass(Driver.class);
		job.setMapperClass(RatingMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TextInputFormat.setInputPaths(job, inputPath);
		TextOutputFormat.setOutputPath(job, outputInter);

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputInter))
			hdfs.delete(outputInter, true);

		job.addCacheFile(cacheFile1.toUri());
		boolean code = job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Map-side join");

		if (code) {
			job2.setJarByClass(Driver.class);
			job2.setMapperClass(UserRatingMapper.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			TextInputFormat.setInputPaths(job2, outputInter);
			TextOutputFormat.setOutputPath(job2, outputDir);

			if (hdfs.exists(outputDir))
				hdfs.delete(outputDir, true);

			job2.addCacheFile(cacheFile2.toUri());
			code = job2.waitForCompletion(true);
		}
		System.exit(code ? 0 : 1);
	}

}
