package com.sample.designpattern.resources;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomDataGeneratorRecordReader extends RecordReader<Text, NullWritable> {

	
	private int numRecordToCreate = 0;
	private int createdRecords =0;
	private Text key = new Text();
	private NullWritable value = NullWritable.get();
	private Random rnd = new Random();
	private String pattern = "dd-MM-yyyy";
	private SimpleDateFormat sdf = new SimpleDateFormat(pattern);
	List<String>  randomWords = new ArrayList<>();
	
	@Override
	public void close() throws IOException {
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float) createdRecords / (float)numRecordToCreate;
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		this.numRecordToCreate = context.getConfiguration().getInt(FakeFileInputFormatClass.NUM_RECORDS_PER_TASK, -1);
		
		// get number of random words from distributed cache 
		URI[] file = DistributedCache.getCacheFiles(context.getConfiguration());
		
		
		BufferedReader bufferedReader = new BufferedReader(new FileReader(file[0].toString()));
		
		String line = "";
		while((line = bufferedReader.readLine()) != null){
			randomWords.add(line);
		}
		
		bufferedReader.close();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		if(createdRecords < numRecordToCreate){
			
			int score = Math.abs(rnd.nextInt()) % 15000;
			int rowId = Math.abs(rnd.nextInt()) % 10000000;
			int postId = Math.abs(rnd.nextInt()) % 100000;
			int userId = Math.abs(rnd.nextInt()) % 10000;
			String text = randomWords.get(Math.abs(rnd.nextInt()) % randomWords.size());
			
			String finalText = score +"\t"+ rowId + "\t" + postId + "\t" + userId + "\t" + text;
			
			key.set(finalText);
			
			++createdRecords;
			
			return true;
			
		}else{
			return false;
		}
		
	}

}
