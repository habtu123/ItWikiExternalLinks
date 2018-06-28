import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hcatalog.data.HCatRecord;

public class ExternalLinks {

	public class ExternalLinksMapper extends Mapper<Text, Text, Text, IntWritable> {
		
		public void map(Object key, HCatRecord value, Context context) throws IOException, InterruptedException {
			
		}
	}
}
