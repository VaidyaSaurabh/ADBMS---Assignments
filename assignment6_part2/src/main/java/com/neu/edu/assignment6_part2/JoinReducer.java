package com.neu.edu.assignment6_part2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

	private static final Text EMPTY_TEXT = new Text("");
	private Text tmp = new Text();
	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();
	private String joinType = null;

	public void setup(Context context) {
		// Get the type of join from our configuration
		joinType = context.getConfiguration().get("join.type");
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Clear our lists
		listA.clear();
		listB.clear();
		while (values.iterator().hasNext()) {
			tmp = values.iterator().next();

			if (tmp.charAt(0) == 'A') {
				listA.add(new Text(tmp.toString().substring(1)));
			} else if (tmp.charAt(0) == 'B') {
				listB.add(new Text(tmp.toString().substring(1)));
			}
		}
		executeJoinLogic(context);
	}

	private void executeJoinLogic(Context context) throws IOException, InterruptedException {

		if (!listA.isEmpty() && !listB.isEmpty()) {
			for (Text A : listA) {
				for (Text B : listB) {
					context.write(A, B);
				}
			}
		}
	}
}