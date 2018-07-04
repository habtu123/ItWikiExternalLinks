package it.wiki.hadoop.map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import it.wiki.hadoop.externallinks.*;

public class ExtLinksTwo {

	 public static void main(String[] args) {
	        try { 
	            Configuration conf = new Configuration();
	 
	            conf.set("START_TAG_KEY", "<page>");
	            conf.set("END_TAG_KEY", "</page>");
	            conf.set("mapredudce.textoutputformat.separatorText", ",");
	            
	            Job job = Job.getInstance(conf, "");
	            FileInputFormat.addInputPath(job, new Path(args[0]));
	            FileOutputFormat.setOutputPath(job, new Path(args[1]));
	            job.setJarByClass(ExtLinks.class);	            
	    
	            job.setMapperClass(MapperTwo.class);
	            job.setCombinerClass(ReducerTwo.class);
	            job.setReducerClass(ReducerTwo.class);
	            
	            job.setInputFormatClass(XMLInputFormat.class);
	            job.setOutputValueClass(IntWritable.class);
	 
	            job.setMapOutputKeyClass(Text.class);
	            job.setMapOutputValueClass(IntWritable.class);
	            	           
	            job.setOutputKeyClass(Text.class);
	            job.setOutputValueClass(IntWritable.class);
         
	            job.waitForCompletion(true);
	 
	        } catch (Exception e) {
	            System.out.println(e.getMessage().toString());
	        }
	 
	    }
}
