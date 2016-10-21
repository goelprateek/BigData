package com.sample.agregaion.keyvalue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class MinMaxCountTouple implements Writable {
	
	private long count;
	private Date min ;
	private Date max;
	private final static String pattern = "dd-MM-yyyy"; 
	private final static SimpleDateFormat sdf = new SimpleDateFormat(pattern);
	
	
	
	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public Date getMin() {
		return min;
	}

	public void setMin(Date min) {
		this.min = min;
	}

	public Date getMax() {
		return max;
	}

	public void setMax(Date max) {
		this.max = max;
	}

	public void readFields(DataInput dataInput) throws IOException {
		min = 	new Date(dataInput.readLong());
		max = new Date(dataInput.readLong());
		count = dataInput.readLong();
	}

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(min.getTime());
		dataOutput.writeLong(min.getTime());
		dataOutput.writeLong(count);
	}

	@Override
	public String toString() {
		return sdf.format(min) + "\t" +sdf.format(max) + "\t" + count;
	}
	
	

}
