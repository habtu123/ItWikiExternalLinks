package it.wiki.hadoop.map;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapperThree{

	public class MyMapperThree extends Mapper<Object, Text, Text, IntWritable>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{		
			String[] links = value.toString().split(",");		
			context.write(new Text(links[1]), new IntWritable(1));
		}
	} // end of MyMapper Threee
	
	public class MyReducerThree extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reducer(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			
			int result = 0; 
			
			for(IntWritable val: values)
			{
				result += val.get();
			}
			
			context.write(key, new IntWritable(result));
		}
	}// end of MyReducerThree class
	
	public static void main(String[] args) throws Exception {
		
		if(args.length < 2) {
			System.out.println("Minium number of inputs not given");
			System.exit(1);
		}
		 Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf);
		 job.setJarByClass(MapperThree.class);
	    job.setMapperClass(MyMapperThree.class);
	    job.setCombinerClass(MyReducerThree.class);
	    job.setReducerClass(MyReducerThree.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
