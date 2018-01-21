package com.neu.edu.assignment4_part3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String val = new String();
		Map<String, Integer> ipCounter = new HashMap<String, Integer>();
		for (Text id : values) {
			String keyString = id.toString();
			if (ipCounter.containsKey(keyString)) {
				Integer count = new Integer(ipCounter.get(keyString).intValue() + 1);
				ipCounter.put(keyString, count);
			} else {
				ipCounter.put(keyString, new Integer(1));
			}
		}
		for (Map.Entry<String, Integer> entry : ipCounter.entrySet()) {
			val = val + entry.getKey() + "-" + entry.getValue() + "  ";
		}
		result.set(val);
		context.write(key, result);
	}

}
