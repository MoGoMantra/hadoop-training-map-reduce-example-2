/** 
 * Copyright 2015 MoGo Mantra Inc <http://www.mogomantra.com> 
 *  
 * Created Mar 20, 2015 
 * 
 * Last edited by:      $Author: $  
 *             on:      $Date: $  
 *       Filename:      $Id: $ 
 *       Revision:      $Rev: $ 
 *            URL:      $URL: $ 
 */
package com.mogo.training.hadoop.example2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author mohangoyal
 * 
 */
public class GenerateUniqueIdSecondPassMapper extends
		Mapper<Text, Text, LongWritable, Text> {

	Map<String, Long> offsets = new HashMap<String, Long>();
	static LongWritable outkey = new LongWritable(0);

	/**
	 * This method will be called once. It will performed following actions 1)
	 * get the partition id of map split 2) Create a New file in HDFS to store
	 * the values.
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		// Get the configuration
		Configuration conf = context.getConfiguration();

		int numvals = 0;
		numvals = conf.getInt(Constants.PARAMETER_CUMSUM_NUMVALS, 0);
		offsets.clear();
		for (int i = 0; i < numvals; i++) {
			String val = conf.get(Constants.PARAMETER_CUMSUM_NTHVALUE + i);
			String[] parts = val.split("\t");
			offsets.put(parts[0], Long.parseLong(parts[2]));
		}
	}

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		//System.out.println("Second MapReduce Example Map Input Key : " + key
				//+ "\t value : " + value);

		String[] parts = key.toString().split(";");
		long rownum = Long.parseLong(parts[1]) + offsets.get(parts[0]);
		outkey.set(rownum);
		context.write(outkey, value);
	
	}

}
