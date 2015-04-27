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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author mohangoyal
 * 
 */
public class GenerateUniqueId {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("Usage: Generate Unique Id <input path> <tmp path> <output path>");
			System.exit(-1);
		}

		String workdir = args[1];
		String keySummaryOutput = workdir + "/pass1_keysummary";
		String recordsOutput = workdir + "/pass1_records";
		String finalConfOutput = workdir + "/results";;

		Job newKeyJob = Job.getInstance();
		newKeyJob.setJarByClass(GenerateUniqueId.class);
		newKeyJob.setJobName("Compute Unique Id");
		
		Configuration newKeyJobConf = newKeyJob.getConfiguration();

		// Before running the job, delete the output files
		FileSystem fs = FileSystem.get(newKeyJobConf);
		//fs.delete(new Path(workdir), true);
		fs.delete(new Path(finalConfOutput), true);


		// the directory location in configuration so that Mapper can access this.
		newKeyJobConf.set(Constants.PARAMETER_SAVE_RECORDS_DIR, recordsOutput);

		
		FileInputFormat.addInputPath(newKeyJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(newKeyJob, new Path(keySummaryOutput));

		newKeyJob.setMapperClass(GenerateUniqueIdFirstPassMapper.class);
		newKeyJob.setCombinerClass(GenerateUniqueIdFirstPassReducer.class);
		newKeyJob.setReducerClass(GenerateUniqueIdFirstPassReducer.class);
		
		//set the input format 
		newKeyJob.setInputFormatClass(TextInputFormat.class);
		
		//Set the output type from Map Reduce Job
		newKeyJob.setOutputKeyClass(Text.class);
		newKeyJob.setOutputValueClass(Text.class);
		
		//set the output types for Mapper
		newKeyJob.setMapOutputKeyClass(IntWritable.class);
		newKeyJob.setMapOutputValueClass(LongWritable.class);

		//newKeyJob.waitForCompletion(true);
		//System.exit(newKeyJob.waitForCompletion(true) ? 1 : 0);
		
		Job finalJob = Job.getInstance();
		finalJob.setJarByClass(GenerateUniqueId.class);
		finalJob.setJobName("Finalize and Assign Unique Id");
		
		Configuration finalConf = finalJob.getConfiguration();

		// Now read the results in the key summary file 
		FileStatus[] files = fs.globStatus(new Path(keySummaryOutput+ "/p*"));
		int numvals = 0;
		long cumsum = 0;
		for (FileStatus fstat : files) {
			FSDataInputStream fsdis = fs.open(fstat.getPath());
			String line = "";
			while ((line = fsdis.readLine()) != null) {
				finalConf.set(Constants.PARAMETER_CUMSUM_NTHVALUE + numvals++, line + "\t" + cumsum);
				String[] vals = line.split("\t");
				cumsum += Long.parseLong(vals[1]);
			}
		}
		finalConf.setInt(Constants.PARAMETER_CUMSUM_NUMVALS, numvals);
		
		// This code block just prints out the offsets
		//for (int i = 0; i < numvals; i++) {
			//System.out.println("cumsum[" + i + "] = "
			//		+ finalConf.get(Constants.PARAMETER_CUMSUM_NTHVALUE + i));
		//}
		
		//String finalConfOutput = args[2];
		
		//set the output keys
		finalJob.setOutputKeyClass(LongWritable.class);
		finalJob.setOutputValueClass(Text.class);
		finalJob.setMapOutputKeyClass(LongWritable.class);
		finalJob.setMapOutputValueClass(Text.class);
		
		finalJob.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(finalJob, new Path(recordsOutput));
		FileOutputFormat.setOutputPath(finalJob, new Path(finalConfOutput));
		finalJob.setMapperClass(GenerateUniqueIdSecondPassMapper.class);

		finalJob.waitForCompletion(true);
		
	}

}
