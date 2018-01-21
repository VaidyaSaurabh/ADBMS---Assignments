package com.neu.edu.assignment2_part4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CustomWritable implements Writable, WritableComparable<CustomWritable> {

	private String ssymbol;
	private String date;
	private float stockedprice;
	private long svolume;

	public CustomWritable() {

	}

	public CustomWritable(String date, long svolume, float stockedprice) {
		this.date = date;
		this.svolume = svolume;
		this.stockedprice = stockedprice;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public long getStockVolume() {
		return svolume;
	}

	public void setStockVolume(long svolume) {
		this.svolume = svolume;
	}

	public float getMaxAdjClose() {
		return stockedprice;
	}

	public void setMaxAdjClose(float stockedprice) {
		this.stockedprice = stockedprice;
	}

	public void write(DataOutput d) throws IOException {
		WritableUtils.writeString(d, date);
		d.writeLong(svolume);
		d.writeFloat(stockedprice);
	}

	public void readFields(DataInput di) throws IOException {
		date = WritableUtils.readString(di);
		svolume = di.readLong();
		stockedprice = di.readFloat();
	}

	public int compareTo(CustomWritable o) {
		return -1 * (date.compareTo(o.date));
	}

}
