package com.sample.agregaion.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapred.lib.ChainMapper;

public class CompositeJoinMapper extends MapReduceBase
		implements org.apache.hadoop.mapred.Mapper<Text, TupleWritable, Text, Text> {

	@Override
	public void map(Text arg0, TupleWritable arg1, OutputCollector<Text, Text> outputCollector, Reporter arg3) throws IOException {
		
		// Get the first two elements in the tuple and output them
		outputCollector.collect((Text)arg1.get(0), (Text)arg1.get(1));
	}

}
