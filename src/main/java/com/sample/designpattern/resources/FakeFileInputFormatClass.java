package com.sample.designpattern.resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FakeFileInputFormatClass extends InputFormat<Text, NullWritable> {
	
	static final String NUM_MAP_TASK = "mapreduce.random.generator.map.task";
	static final String NUM_RECORDS_PER_TASK = "mapreduce.random.generator.num.record.per.task";
	static final String RANDOM_WORD_LIST = "mapreduce.random.generator.random.word.file";
	
	@Override
	public RecordReader<Text, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		RandomDataGeneratorRecordReader dataGeneratorRecordReader = new RandomDataGeneratorRecordReader();
		dataGeneratorRecordReader.initialize(inputSplit, context);
		
		return dataGeneratorRecordReader;
	}
	
	@Override
	public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
		
		int numSplits = jobContext.getConfiguration().getInt(NUM_MAP_TASK, -1);
		
		List<InputSplit> splits = new ArrayList<InputSplit>(); 
		
		for(int i = 0 ; i < numSplits; i++){
			splits.add(new FakeInputSplit());
		}
		
		return splits;
	}
	
	public static void setNumMaptask(Job job, int numMapTask){
		job.getConfiguration().setInt(NUM_MAP_TASK, numMapTask);
	}
	
	public static void setNumRecordPerTask(Job job , int numRecordPertask){
		job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, numRecordPertask);
	}
	
	public static void setRandomWordList(Job job , Path file){
		DistributedCache.addCacheFile(file.toUri(), job.getConfiguration());
	}

	

}
