package com.sample.agregaion.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalOrderPartionerAnalyzerMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String sortedkey = "lastaName";
		
		context.write(new Text(sortedkey), value);
		
	}
	
	

}
