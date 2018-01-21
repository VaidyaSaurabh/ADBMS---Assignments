package com.neu.edu.assignment3_part2;

import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

class StWritable implements Writable{
    
    private double average = 0;
    private long count = 0;

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
    
    public void readFields(DataInput in) throws IOException {
        
        average = in.readDouble();
        count = in.readLong();
    }
    
    public void write(DataOutput out) throws IOException {
        out.writeDouble(average);
        out.writeLong(count);
    }
    
    public String toString() {
        
        return "Average: " + average;
    }
}
