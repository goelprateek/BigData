package com.sample.agregation.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;

import com.sample.agregaion.reducer.ValueReducer;
import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class TotalOrderPartitinerDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration configuration = new Configuration();
		
		Path inpath = new Path(arg0[0]);
		Path partitionfile = new Path(arg0[1]+"_partition.lst");
		Path outputStage = new Path(arg0[1]+"_staging");
		Path outputOrder = new Path(arg0[1]);
		
		Job job = Job.getInstance(configuration, "TotalOrderPartinioning");
		job.setJarByClass(TotalOrderPartitinerDriver.class);
		
		job.setMapperClass(org.apache.hadoop.mapreduce.Mapper.class);
		job.setNumReduceTasks(0);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		TextInputFormat.setInputPaths(job, inpath);
		
		// set output format to sequence file 
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputStage);
		
		
		int result = job.waitForCompletion(true)? 0:1;
		
		if(result == 0){
			Job job2 = Job.getInstance(configuration,"TotalOrderSortingStage");
			job2.setJarByClass(TotalOrderPartitinerDriver.class);
			
			// setting identity mapper to job2
			job2.setMapperClass(Mapper.class);
			// setting identity reducer to job2
			job2.setReducerClass(ValueReducer.class);
			// setting partition class
			job2.setPartitionerClass(TotalOrderPartitioner.class);
			
			// setting partition file to TotalOrderPartitioner 
			TotalOrderPartitioner.setPartitionFile(configuration, partitionfile);
			
			// set the input format for job 2
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.setInputPaths(job2, outputStage);
			
			// set the output format of job2 
			TextOutputFormat.setOutputPath(job2, outputOrder);
			
			// set seperator to empty string
			job2.getConfiguration().set("map.textoutputformat.seperator", "");
			
			// use InputSample to go through the output of previous job , sample it and create a partition file 
			InputSampler.writePartitionFile(job2, new InputSampler.RandomSampler(.001, 1000));
			
			// start the job and wait for completion
			result  = job2.waitForCompletion(true) ? 0:1;
			
		}
		
		FileSystem fileSystem = FileSystem.get(new Configuration());
		fileSystem.delete(partitionfile, false);
		fileSystem.delete(outputStage, true);
		
		return result;
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
