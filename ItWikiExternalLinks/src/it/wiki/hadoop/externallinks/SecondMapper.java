package it.wiki.hadoop.externallinks;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SecondMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
	private final static IntWritable ONE = new IntWritable(1);
	private Text link = new Text();
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] line; 
		line = key.toString().split(",");
		link.set(line[1]);
		
		context.write(link, ONE);
	}
}
