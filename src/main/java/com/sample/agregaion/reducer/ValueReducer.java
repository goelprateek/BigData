package com.sample.agregaion.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ValueReducer extends Reducer<Text, Text, Text, NullWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
			
		
		for (Text text : arg1) {
			context.write(text, NullWritable.get());
		}
		
	}

	
	
}
