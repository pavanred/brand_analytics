package ids594.ba.dataprocessing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class DataProcessDriver extends Configured implements Tool {

	/**
	 * @author asif
	 */
	public static void main(String[] args) {
		
		int res = 0;
		try {			
			res = ToolRunner.run(new Configuration(), new DataProcessDriver(), args);			
		} catch (Exception e) {			
			e.printStackTrace();
		}
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJobName("Remove Duplicates");
		
		job.setJarByClass(DataProcessDriver.class);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setMapperClass(DuplicateMapper.class);
		job.setReducerClass(DuplicateReducer.class);
		
		// Setting to KeyValue Text Input format class, so that the key and value is text
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		return 0;
	}
}
