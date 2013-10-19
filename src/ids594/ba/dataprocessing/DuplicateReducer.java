package ids594.ba.dataprocessing;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class DuplicateReducer extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		String[] keyvalues = key.toString().split(",");
		context.write(new Text(keyvalues[1]), new Text(keyvalues[0]));		
	}
}
