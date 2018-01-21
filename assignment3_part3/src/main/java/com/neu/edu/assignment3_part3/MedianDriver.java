package com.neu.edu.assignment3_part3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MedianDriver {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "StandardMedian");
		job.setJarByClass(MedianDriver.class);
		job.setMapperClass(StandardMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(StandardReducer.class);

		// Output
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MedianCustomWritable.class);

		// Input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (InterruptedException | ClassNotFoundException ex) {
			return;
		}
	}

	public static class StandardMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		IntWritable MovieId = new IntWritable();
		IntWritable Rating = new IntWritable();

		@Override
		public void map(Object key, Text value, Context context) {
			try {
				String arr[] = value.toString().split("::");
				int movie = Integer.parseInt(arr[1]);
				int rat = Integer.parseInt(arr[2]);
				MovieId.set(movie);
				Rating.set(rat);
				context.write(MovieId, Rating);

			} catch (NumberFormatException | IOException | InterruptedException e) {

			}
		}
	}

	public static class StandardReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianCustomWritable> {

		private MedianCustomWritable result = new MedianCustomWritable();
		private ArrayList<Integer> arrayList = new ArrayList<>();

	
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			double sum = 0;
			double count = 0;
			arrayList.clear();
			result.setStandardDeviation(0);

			for (IntWritable val : values) {
				arrayList.add(val.get());
				sum += val.get();
				count++;
			}

			Collections.sort(arrayList);

			if (count % 2 == 0) {
				result.setMedian((arrayList.get((int) count / 2) + arrayList.get((int) count / 2 - 1)) / 2);

			} else {
				result.setMedian(arrayList.get((int) count / 2));
			}

			double mean = sum / count;
			double sumOfSquares = 0;

			for (double val : arrayList) {
				sumOfSquares += (val - mean) * (val - mean);
			}
			result.setStandardDeviation((double) Math.sqrt(sumOfSquares / (count - 1)));
			context.write(key, result);
		}
	}

}
