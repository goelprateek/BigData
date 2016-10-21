package com.sample.agregaion.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MeanStdDevMapper extends Mapper<Text, Text, Text, Text>{

	private MultipleOutputs<Text, Text> mos = null;
	
	@Override
	protected void cleanup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		mos.close();
	}

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
//		mos.write(namedOutput, key, value, baseOutputPath);
		
		
		
	}

	@Override
	protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<>(context);
	}
	
	

}
