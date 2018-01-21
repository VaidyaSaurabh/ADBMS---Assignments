package com.neu.edu.assignment3_part3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

class MedianCustomWritable implements Writable {

	private double medianValue;
	private double standardDeviationValue;

	public double getMedian() {
		return medianValue;
	}

	public void setMedian(double median) {
		this.medianValue = median;
	}

	public double getStandardDeviation() {
		return standardDeviationValue;
	}

	public void setStandardDeviation(double standardDeviation) {
		this.standardDeviationValue = standardDeviation;
	}

	public void write(DataOutput a) throws IOException {
		a.writeDouble(medianValue);
		a.writeDouble(standardDeviationValue);
	}

	public void readFields(DataInput b) throws IOException {
		medianValue = b.readDouble();
		standardDeviationValue = b.readDouble();
	}

	@Override
	public String toString() {
		return ("Median : " + this.getMedian() + "\t" + "Standard Deviation : " + this.getStandardDeviation());
	}
}
