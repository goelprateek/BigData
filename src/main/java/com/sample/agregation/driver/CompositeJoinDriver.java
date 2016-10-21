package com.sample.agregation.driver;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

/**
 * 
 * @author shivanand
 * 
 * Requirement for composite join 
 * This i smap side join , so no combiner,shuffle,sort , reducer required 
 * 1) an Inner or full join desired  
 * 2) All data sets are sufficiently large 
 * 3) All data set can be read using foreign key as input key to the mapper 
 * 4) all data set have same no. of partitions 
 * 5) Each partition is sorted by foreign key , all foreign key reside in same partition of each data set 
 * i.e partition X of dataset A and B contains the same foreign key and these foreign keys are presented 
 *  only on partition X
 * 6) Data set do not change often (If they have to be preapred   
 *
 *
 *
 */
public class CompositeJoinDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		
		Path userPath = new Path(arg0[0]);
		Path commentPath = new Path(arg0[1]);
		Path outputDir = new Path(arg0[2]);
		String joinType = arg0[3];
		
		
		
		/* this is to get cluster properties
		 * 
		Configuration configuration = new Configuration();
		Cluster cluster = new Cluster(configuration);
		ClusterMetrics clusterStatus = cluster.getClusterStatus();
		clusterStatus.getMapSlotCapacity();*/
		
		JobConf conf = new  JobConf("CompositeJoin");
		
		conf.setJarByClass(CompositeJoinDriver.class);
		conf.setMapperClass(org.apache.hadoop.mapred.Mapper.class);
		
		conf.setInputFormat(CompositeInputFormat.class);
		conf.set("mapred.join.expr", CompositeInputFormat.compose(joinType, KeyValueTextInputFormat.class, userPath, commentPath));
		TextOutputFormat.setOutputPath(conf, outputDir);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		RunningJob runJob = JobClient.runJob(conf);
		
		while(!runJob.isComplete()){
			Thread.sleep(100);
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new Configuration(), new CompositeJoinDriver(), args);

	}

}
