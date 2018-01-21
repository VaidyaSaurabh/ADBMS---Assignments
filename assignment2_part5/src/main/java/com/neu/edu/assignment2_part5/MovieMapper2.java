package com.neu.edu.assignment2_part5;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper2 extends Mapper<LongWritable, Text, CustomWritableMovie, IntWritable> {

	@Override
	protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		LongWritable movieId = new LongWritable();
		FloatWritable rating = new FloatWritable();
		if (values.toString().length() > 0) {
			try {
				String str[] = values.toString().split("\t");
				movieId.set(Long.parseLong(str[0]));
				rating.set(Float.parseFloat(str[1]));

				CustomWritableMovie data = new CustomWritableMovie(movieId.get(), rating.get());
				context.write(data, new IntWritable(1));
			} catch (IOException | InterruptedException ex) {
				Logger.getLogger(MovieMapper2.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}
}
