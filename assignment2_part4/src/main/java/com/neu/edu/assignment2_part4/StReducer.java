package com.neu.edu.assignment2_part4;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StReducer extends Reducer<Text, CustomWritable, NullWritable, Text> {

	private long minimumValue;
	private String minDate;
	private long maximumValue;
	private String maxDate;
	private float stockedprice;
	private Text result = new Text();

	@Override
	protected void reduce(Text key, Iterable<CustomWritable> values, Context context)
			throws IOException, InterruptedException {
		minimumValue = Long.MAX_VALUE;
		maximumValue = Long.MIN_VALUE;
		stockedprice = Long.MIN_VALUE;
		for (CustomWritable value : values) {
			if (minimumValue > value.getStockVolume()) {
				minimumValue = value.getStockVolume();
				minDate = value.getDate();
			}
			if (maximumValue < value.getStockVolume()) {
				maximumValue = value.getStockVolume();
				maxDate = value.getDate();
			}
			if (stockedprice < value.getMaxAdjClose()) {
				stockedprice = value.getMaxAdjClose();
			}
		}
		result.set(key + " Max Date : " + maxDate + " Min Date : " + minDate
				+ " Max Stock_price_adj_close : " + stockedprice);

		context.write(NullWritable.get(), result);

	}
}