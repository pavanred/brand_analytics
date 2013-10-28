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

public class AggregateData extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text > {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
				String[] KeyValue = value.toString().trim().split("\\t");
				
				context.write(new Text(KeyValue[0]), new Text(KeyValue[1]));


				/*
				if(KeyValue.length == 2)
				{
					context.write(new Text(KeyValue[1]+ "AA"), new Text(KeyValue[1]+" BB"));
				}
				*/
		}
	}
				
	
	

	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 
			String brandList = "";
			for (Text val : values) {
				brandList += val + " ";
			}
			brandList = brandList.trim();
			context.write(new Text(brandList) , new Text(""));

		}
	}
	
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "AggregateData");
		job.setJarByClass(AggregateData.class);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                //job.setOutputKeyClass(Text.class);
                //job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AggregateData(), args);
		System.exit(res);
	}
}

