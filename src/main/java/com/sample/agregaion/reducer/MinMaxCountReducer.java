package com.sample.agregaion.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sample.agregaion.keyvalue.MinMaxCountTouple;

public class MinMaxCountReducer extends Reducer<Text, MinMaxCountTouple, Text, MinMaxCountTouple> {

	private MinMaxCountTouple value = new MinMaxCountTouple(); 
	



	@Override
	protected void reduce(Text arg0, Iterable<MinMaxCountTouple> arg1,
			Reducer<Text, MinMaxCountTouple, Text, MinMaxCountTouple>.Context context)
			throws IOException, InterruptedException {
		
		
		int sum = 0;
		
		value.setMax(null);
		value.setMin(null);
		value.setCount(0);
		
		
		for(MinMaxCountTouple touple : arg1){
			
			if(touple.getMin() == null || touple.getMin().compareTo(touple.getMin()) < 0){
				value.setMin(touple.getMin());
			}
			if(touple.getMax() == null || touple.getMax().compareTo(touple.getMax()) > 0){
				value.setMax(touple.getMax());
			}
			
			sum += touple.getCount();
			
		}
		
		value.setCount(sum);
		
		context.write(new Text(), value);
	}
	
	
	
	
}
