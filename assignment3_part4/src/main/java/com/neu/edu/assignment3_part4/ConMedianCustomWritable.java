package com.neu.edu.assignment3_part4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ConMedianCustomWritable implements Writable {

	private double medianValue;
	private double standardDeviationValue;

	public double getMedian() {
		return medianValue;
	}

	public void setMedian(double median) {
		this.medianValue = median;
	}

	public double getStddev() {
		return standardDeviationValue;
	}

	public void setStddev(double stddev) {
		this.standardDeviationValue = stddev;
	}

	public void write(DataOutput a) throws IOException {
		a.writeDouble(medianValue);
		a.writeDouble(standardDeviationValue);
	}

	public void readFields(DataInput b) throws IOException {
		medianValue = b.readDouble();
		standardDeviationValue = b.readDouble();

	}

	public String toString() {
		return ("Median : " + String.valueOf(this.medianValue) + "\t" + "Standard Deviation : "
				+ String.valueOf(this.standardDeviationValue));

	}

}