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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author mohangoyal
 * 
 */
public class GenerateUniqueIdFirstPassReducer extends
		Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

	@Override
	public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		//System.out.println("First MapReduce Example Reduce Input Key : "+key+"\t value : "+values );
		long maxValue = Long.MIN_VALUE;
		for(LongWritable value: values) {
			maxValue = Math.max(maxValue,  value.get());
		}
		context.write(key,  new LongWritable(maxValue));
	}

}
