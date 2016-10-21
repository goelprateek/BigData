package com.sample.agregaion.mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.sample.agregaion.keyvalue.MinMaxCountTouple;

public class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTouple> {
	
	private MinMaxCountTouple minMaxCountTouple = new MinMaxCountTouple();
	private Text key = new Text();
	private Splitter splitter;
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy"); 
	
	
	@Override
	protected void setup(Mapper<Object, Text, Text, MinMaxCountTouple>.Context context)
			throws IOException, InterruptedException {
		
		Configuration configuration = context.getConfiguration();
		String seperator = configuration.get("map.reduce.seperator");
		splitter = Splitter.on(seperator).trimResults();
		
		
	}




	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, MinMaxCountTouple>.Context context)
			throws IOException, InterruptedException {
		
		Iterable<String> split = splitter.split(value.toString());
		
		String userId = Iterables.get(split, 0);
		this.key.set(userId);
		
		String creationDate = Iterables.get(split, 1);
		
		Date parse = null;
		
		try {
		
			parse = sdf.parse(creationDate);

		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		minMaxCountTouple.setMin(parse);
		minMaxCountTouple.setMax(parse);
		minMaxCountTouple.setCount(1);
		
		
		
		context.write(this.key, minMaxCountTouple);
	}
}
