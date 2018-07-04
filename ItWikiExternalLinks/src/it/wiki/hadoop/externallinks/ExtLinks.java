package it.wiki.hadoop.externallinks;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ExtLinks {

	 public static void main(String[] args) {
	        try { 
	            Configuration conf = new Configuration();
	            // conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, 2048);
	            // OR alternatively you can set it this way, the name of the
	            // property is
	            // "mapreduce.input.fixedlengthinputformat.record.length"
	            // conf.setInt("mapreduce.input.fixedlengthinputformat.record.length",
	            // 2048);
	            //String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();
	 
	            conf.set("START_TAG_KEY", "<page>");
	            conf.set("END_TAG_KEY", "</page>");
	//            conf.set("mapredudce.textoutputformat.separatorText", ",");
	            
	            Job job = Job.getInstance(conf, "XML Processing Processing");
	            FileInputFormat.addInputPath(job, new Path(args[0]));
	            FileOutputFormat.setOutputPath(job, new Path(args[1]));
	            job.setJarByClass(ExtLinks.class);
	            
//	            Configuration map1Conf = new Configuration(false);
//////	            
//////	            
//	            ChainMapper.addMapper(job, MyMapper.class, LongWritable.class, 
//	            		Text.class, Text.class, IntWritable.class, map1Conf);
////	            
//	            Configuration map2Conf = new Configuration(false);
////	            
//	            ChainMapper.addMapper(job, SecondMapper.class, Text.class, 
//	            		IntWritable.class, Text.class, IntWritable.class, map2Conf);
	            
	           
	            
	           
	            job.setMapperClass(MyMapper.class);
//	            job.setReducerClass(MyReducer.class);
//	            job.setMapperClass(SecondMapper.class);
	        
	            job.setNumReduceTasks(0);
	            
	            job.setInputFormatClass(XMLInputFormat.class);
	            job.setOutputValueClass(IntWritable.class);
	 
	            job.setMapOutputKeyClass(Text.class);
	            job.setMapOutputValueClass(IntWritable.class);
	            	           
	            job.setOutputKeyClass(Text.class);
	            job.setOutputValueClass(IntWritable.class);
            
	            job.waitForCompletion(true);
	 
	        } catch (Exception e) {
	           // LogWriter.getInstance().WriteLog("Driver Error: " + e.getMessage());
	            System.out.println(e.getMessage().toString());
	        }
	        // job.setReducerClass(ClickReducer.class);
	 
	    }
}
