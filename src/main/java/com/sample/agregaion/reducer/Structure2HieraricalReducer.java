/**
 * 
 */
package com.sample.agregaion.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author shivanand
 *
 */
public class Structure2HieraricalReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	

}
