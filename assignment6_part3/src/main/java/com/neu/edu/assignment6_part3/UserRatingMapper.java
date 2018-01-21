package com.neu.edu.assignment6_part3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserRatingMapper extends Mapper<Object, Text, Text, Text> {

	private HashMap<String, String> userIdInfo = new HashMap<String, String>();
	private Text outValue = new Text();

	@SuppressWarnings("resource")
	public void setup(Context context) throws IOException, InterruptedException {
		URI[] files = context.getCacheFiles();
		for (URI p : files) {
			BufferedReader rdr = new BufferedReader(new FileReader(p.toString()));
			String line = null;
			while ((line = rdr.readLine()) != null) {
				String[] fields = line.split(";");
				String userId = fields[0];
				userIdInfo.put(userId, line);
			}
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(";");
		String userId = fields[0];
		if (userId != null && userIdInfo.get(userId) != null) {
			outValue.set(userIdInfo.get(userId));
			context.write(new Text(value.toString()), outValue);
		}
	}

}
