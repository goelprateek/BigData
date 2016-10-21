package com.sample.agregaion.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sample.designpattern.resources.FakeFileInputFormatClass;
import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class RandomDataGeneratorDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		
		
		Configuration configuration = new Configuration();
		
		int numMapTask = Integer.parseInt(arg0[0]);
		int numRecordPerTask = Integer.parseInt(arg0[1]);
		Path wordList = new Path(arg0[2]);
		Path outDirectory = new Path(arg0[3]);
		
		
		Job job = Job.getInstance(configuration,"RandomDataGenerator");
		job.setJarByClass(RandomDataGeneratorDriver.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(FakeFileInputFormatClass.class);
		FakeFileInputFormatClass.setNumMaptask(job, numMapTask);
		FakeFileInputFormatClass.setNumRecordPerTask(job, numRecordPerTask);
		FakeFileInputFormatClass.setRandomWordList(job, wordList);
		
		TextOutputFormat.setOutputPath(job, outDirectory);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		return job.waitForCompletion(true)?0:1;
		
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new RandomDataGeneratorDriver(), args);

	}

}
