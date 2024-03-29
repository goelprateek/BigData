package com.sample.designpattern.resources;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit implements Writable{

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[0];
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		
	}

}
