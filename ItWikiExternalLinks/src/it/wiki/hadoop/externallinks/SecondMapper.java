package it.wiki.hadoop.externallinks;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SecondMapper extends Mapper<Object, Text, Text, Text> {
	private final static IntWritable ONE = new IntWritable(1);
	private Text link = new Text();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] line; 
		line = value.toString().split("\t");
		link.set(line[1]);
		
		//context.write(link, ONE);
	}
}
