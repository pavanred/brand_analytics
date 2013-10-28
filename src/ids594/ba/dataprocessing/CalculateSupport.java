package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.*;

public class CalculateSupport extends Configured implements Tool {
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable > {
		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String[] itemset = value.toString().split(" ");
				Arrays.sort(itemset);
			
				int length = itemset.length;

				for (int i=0; i<length; i++ ) {
					
					for (int j=i+1; j< length; j++ ) {
				
						String item2 = itemset[i].trim() + "," + itemset[j].trim();
						context.write(new Text(item2), one);
					}
				}
				
		}
	}
	
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			 int sum = 0;
			 for (IntWritable val : values) {
			        sum += val.get();
     			 }
		         result.set(sum);
			 context.write(key, result);

		}
	}
	
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "CalculateSupport");
		job.setJarByClass(CalculateSupport.class);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		//job.setInputFormatClass(TextInputFormat.class); 
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CalculateSupport(), args);
		System.exit(res);
	}
}

