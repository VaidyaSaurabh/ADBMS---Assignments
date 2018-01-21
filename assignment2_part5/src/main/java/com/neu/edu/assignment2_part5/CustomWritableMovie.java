package com.neu.edu.assignment2_part5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CustomWritableMovie implements Writable, WritableComparable<CustomWritableMovie>{

    private Long movieId;
    private Float rating;

    
    public CustomWritableMovie()
    {
      
    }
   
    public CustomWritableMovie(Long m, Float r)
    {
        this.movieId = m;
        this.rating = r;
    }

    public Long getMovieId() {
        return movieId;
    }

    public void setMovieId(Long movieId) {
        this.movieId = movieId;
    }

    public Float getTotalRating() {
        return rating;
    }

    public void setTotalRating(Float totalRating) {
        this.rating = totalRating;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeLong(movieId);
        d.writeFloat(rating);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        movieId = di.readLong();
        rating = di.readFloat();
    }

    public int compareTo(CustomWritableMovie o) {
       return -1*(rating.compareTo(o.rating));
    }

    @Override
    public String toString() {
        return movieId + "\t" + rating ;
    }    
    
}