package com.sample.agregaion.mapper;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Splitter;

public class Structure2HieraricalTableBMapper extends Mapper<Text, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();
	private Splitter splitter;
	
	

	@Override
	protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
		Configuration configuration = context.getConfiguration();
		String separator = configuration.get("seperator");
		splitter = Splitter.on(separator);
	}



	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		Iterator<String> iterator = splitter.split(value.toString()).iterator();
		
		String postId = null;
		while(iterator.hasNext()){
			postId = iterator.next();
			outkey.set(postId);
			break;
		}
		
		outvalue.set("C"+value.toString());
		
		context.write(outkey, outvalue);
	}

	
	
	
}
