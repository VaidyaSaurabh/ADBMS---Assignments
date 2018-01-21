package com.neu.edu.assignment3_part4;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ConMedianDriver {

	public static class ConMapper extends Mapper<LongWritable, Text, LongWritable, SortedMapWritable> {

		Long movieId = 0L;
		LongWritable count = new LongWritable(1);
		SortedMapWritable cwo = new SortedMapWritable();
		DoubleWritable rating = new DoubleWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			try {
				String[] str = value.toString().split("::");
				movieId = Long.parseLong(str[1]);
				rating.set(Double.parseDouble(str[2]));

				cwo.put(rating, count);
				context.write(new LongWritable(movieId), cwo);

			} catch (IOException | InterruptedException ex) {
				return;
			}
		}
	}

	public static class ConCombiner extends Reducer<LongWritable, SortedMapWritable, LongWritable, SortedMapWritable> {

		@Override
		protected void reduce(LongWritable key, Iterable<SortedMapWritable> value, Context context)
				throws IOException, InterruptedException {

			SortedMapWritable outval = new SortedMapWritable();
			for (SortedMapWritable v : value) {
				for (Entry<WritableComparable, Writable> entry : v.entrySet()) {
					LongWritable count = (LongWritable) outval.get(entry.getKey());

					if (count != null) {
						count.set(count.get() + ((LongWritable) entry.getValue()).get());
					} else {
						outval.put(entry.getKey(), new LongWritable(((LongWritable) entry.getValue()).get()));
					}
				}
				v.clear();
			}
			context.write(key, outval);
		}
	}

	public static class ConReducer
			extends Reducer<LongWritable, SortedMapWritable, LongWritable, ConMedianCustomWritable> {

		private ConMedianCustomWritable a = new ConMedianCustomWritable();
		private TreeMap<Double, Long> ratingCt = new TreeMap<Double, Long>();

		@Override
		protected void reduce(LongWritable key, Iterable<SortedMapWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			double totalrating = 0;
			ratingCt.clear();
			a.setMedian(0);
			a.setStddev(0);

			for (SortedMapWritable v : values) {
				for (Entry<WritableComparable, Writable> entry : v.entrySet()) {
					double rating = ((DoubleWritable) entry.getKey()).get();
					long cnt = ((LongWritable) entry.getValue()).get();
					totalrating += cnt;
					sum += rating * cnt;

					Long storedCount = ratingCt.get(rating);
					if (storedCount == null) {
						ratingCt.put(rating, cnt);
					} else {
						ratingCt.put(rating, storedCount + cnt);
					}
				}
			}

			double medianIndex = (double) totalrating / 2L;
			double previousRatings = 0;
			double ratings = 0;
			double prevKey = 0;
			for (Entry<Double, Long> entry : ratingCt.entrySet()) {
				ratings = previousRatings + entry.getValue();
				if (previousRatings <= medianIndex && medianIndex < ratings) {
					if (ratings % 2 == 0 && previousRatings == medianIndex) {
						a.setMedian((float) (entry.getKey() + prevKey) / 2.0f);
					} else {
						a.setMedian(entry.getKey());
					}
					break;
				}
				previousRatings = ratings;
				prevKey = entry.getKey();
			}

			// calculate standard deviation
			double mean = (double) sum / totalrating;

			float sumOfSquares = 0.0f;
			for (Entry<Double, Long> entry : ratingCt.entrySet()) {
				sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) * entry.getValue();
			}

			a.setStddev((float) Math.sqrt(sumOfSquares / (totalrating - 1)));
			context.write(key, a);
		}

	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {
		// TODO code application logic here

		try {

			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "ConStandardMedian");
			job.setJarByClass(ConMedianDriver.class);
			job.setMapperClass(ConMapper.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(SortedMapWritable.class);
			job.setCombinerClass(ConCombiner.class);
			job.setReducerClass(ConReducer.class);

			// Output
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(ConMedianCustomWritable.class);
			// Input
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			Path inputPath = new Path(args[0]);
			Path outputDir = new Path(args[1]);

			// Delete output if exists
			FileSystem hdfs = FileSystem.get(conf);
			if (hdfs.exists(outputDir))
				hdfs.delete(outputDir, true);

			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (IOException | InterruptedException | ClassNotFoundException ex) {
			return;
		}

	}

}
