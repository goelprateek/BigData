package com.sample.agregation.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sample.agregaion.mapper.Structure2HieraricalTableAMapper;
import com.sample.agregaion.mapper.Structure2HieraricalTableBMapper;
import com.sample.agregaion.reducer.Structure2HieraricalReducer;
import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class Structure2HieraricalDriver extends Configured implements Tool {

	

	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration configuration = new Configuration();
		
		Job job = Job.getInstance(configuration,"Structure to hierarical job");
		job.setJarByClass(Structure2HieraricalDriver.class);
		
		MultipleInputs.addInputPath(job, new Path(arg0[0]), TextInputFormat.class, Structure2HieraricalTableAMapper.class);
		
		MultipleInputs.addInputPath(job, new Path(arg0[1]), TextInputFormat.class, Structure2HieraricalTableBMapper.class);
		
		job.setReducerClass(Structure2HieraricalReducer.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(arg0[3]));
		
		job.setOutputKeyClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
		
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Structure2HieraricalDriver(), args);

	}

}
