package com.sample.agregation.driver;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sample.agregaion.keyvalue.MinMaxCountTouple;
import com.sample.agregaion.mapper.MinMaxCountMapper;
import com.sample.agregaion.reducer.MinMaxCountReducer;
import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class MinMaxCountDriver extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		
		if(arg0.length < 2 ){
			System.exit(-1);
		}
		
		
		
		Configuration configuration = new Configuration();
		
		Job job = Job.getInstance(configuration,"MinMaxCountJob");
		
		
		job.setJarByClass(MinMaxCountDriver.class);
		job.setMapperClass(MinMaxCountMapper.class);
		job.setCombinerClass(MinMaxCountReducer.class);
		job.setReducerClass(MinMaxCountReducer.class);
		
		job.setMapOutputValueClass(MinMaxCountTouple.class);
		job.setMapOutputKeyClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(MinMaxCountTouple.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		
		int result =  job.waitForCompletion(true) ? 0 :1;
		
		// caculating counters
		if(result == 0){
			CounterGroup group = job.getCounters().getGroup("");
			Iterator<Counter> iterator = group.iterator();
			
			while(iterator.hasNext()){
				Counter next = iterator.next();
				System.out.println(next.getDisplayName());
				System.out.println(next.getValue());
			}
		}
		
		return result;
		
		
	}

	public static void main(String[] args) throws Exception {
	
//		BloomFilter bloomFilter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);
		
		ToolRunner.run(new Configuration(), new MinMaxCountDriver(), args);
	}

}
