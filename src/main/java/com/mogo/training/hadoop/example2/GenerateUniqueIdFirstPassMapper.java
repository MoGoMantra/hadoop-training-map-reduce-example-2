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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author mohangoyal
 * 
 */
public class GenerateUniqueIdFirstPassMapper extends
		Mapper<LongWritable, Text, IntWritable, LongWritable> {

	static SequenceFile.Writer sfw;
	static IntWritable partitionid = new IntWritable(0);
	static Text outkey = new Text("");
	static long localrownum = 0;
	static LongWritable localrownumvalue = new LongWritable(0);
	
	
	
	/**
	 * This method will be called once. It will performed following actions
	 * 		1) get the partition id of map split
	 * 		2) Create a New file in HDFS to store the values.
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		
		// Get the configuration
		Configuration conf = context.getConfiguration();
		
		//get the partition id and store it partitionid varaible
		partitionid.set(conf.getInt("mapred.task.partition", 0));
		
		//get the directory name where we want to save the counts
		String saveRecordsDir = new String(conf.get(Constants.PARAMETER_SAVE_RECORDS_DIR));
		if (saveRecordsDir.endsWith("/")) {
		    saveRecordsDir.substring(0, saveRecordsDir.length() - 1);
		}
		
		//Get HDFS File System and create a sequence file
		try {
			FileSystem fs = FileSystem.get(conf);
			sfw = SequenceFile.createWriter(fs, conf,
					new Path(saveRecordsDir+"/"+String.format("records%05d", partitionid.get())),
					Text.class,	Text.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * This map adds each line to Sequence file as below
	 * partitionId;running sequence number line
	 * Example - 0;20000 value : 999999 26439 SHEEP MOUNTAIN                US US AK PASP  +61812 -147507 +08352
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//System.out.println("First MapReduce Example Map Input Key : "+key+"\t value : "+value );
		
		localrownumvalue.set(++localrownum);
		context.write(partitionid, localrownumvalue);
		outkey.set(partitionid.toString()+";" + localrownum);
		sfw.append(outkey, value);
		
	}

}
