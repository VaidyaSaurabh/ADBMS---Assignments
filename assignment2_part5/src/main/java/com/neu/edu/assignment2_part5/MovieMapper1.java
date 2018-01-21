package com.neu.edu.assignment2_part5;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper1 extends Mapper<LongWritable, Text, LongWritable, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LongWritable movieId = new LongWritable();
        FloatWritable rating = new FloatWritable();
        String str[] = value.toString().split("::");
        int i = 0;

        movieId.set(Long.parseLong(str[1]));
        rating.set(Float.parseFloat(str[2]));

        context.write(movieId, rating);
    }

}

