/** 
 * Copyright 2015 MoGo Mantra Inc <http://www.mogomantra.com> 
 *  
 * Created Apr 25, 2015 
 * 
 * Last edited by:      $Author: $  
 *             on:      $Date: $  
 *       Filename:      $Id: $ 
 *       Revision:      $Rev: $ 
 *            URL:      $URL: $ 
 */
package com.mogo.training.hadoop.example2;

/**
 * @author mohangoyal
 *
 */
public class Constants {
	
	// directory for storing records with new key
	static final String PARAMETER_SAVE_RECORDS_DIR = "rownumbertwopass.saverecordsdir";	
	
	// number of partitions in the first pass
	static final String PARAMETER_CUMSUM_NUMVALS = "rownumbertwopass.cumsum.numvals";	
	
	// offset for each partition
	static final String PARAMETER_CUMSUM_NTHVALUE = "rownumbertwopass.cumsum.";			
	

}
