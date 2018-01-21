package com.neu.edu.assignment6_part2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper3 extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] separatedInput = value.toString().split("\t");

            String ratingString = separatedInput[1];
            if (ratingString == null) {
                return;
            }
            String[] separatedRating = ratingString.toString().split(";");
            String userId = separatedRating[0];
            if (userId == null) {
                return;
            }
            outkey.set(userId);
            outvalue.set("B" + value);
            context.write(outkey, outvalue);
        }
    }