package org.olap4cloud.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataImport {
	
	static class DataImportMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String s = value.toString();
			StringTokenizer st1 = new StringTokenizer(s, "\n", false);
			while(st1.hasMoreTokens()) {
				StringTokenizer st2 = new StringTokenizer(st1.nextToken());
				st2.nextToken();
				int i = Integer.parseInt(st2.nextToken());
				int i2 = Integer.parseInt(st2.nextToken());
				if(i2 != 100)
					continue;
				st2.nextToken();
				st2.nextToken();
				st2.nextToken();
				double d = Double.parseDouble(st2.nextToken());
				context.write(new IntWritable(i), new DoubleWritable(d));
			}
		}
	}
	
	static class DataImportReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>{
		protected void reduce(IntWritable k, Iterable<DoubleWritable> vs,
				org.apache.hadoop.mapreduce.Reducer.Context c)
				throws IOException, InterruptedException {
			double s = 0;
			for(Iterator<DoubleWritable> i = vs.iterator(); i.hasNext(); ) {
				s += i.next().get();
			}
			c.write(k, new DoubleWritable(s));
		}
	}
	
	public static void main(String args[]) throws Exception {
		Job job = new Job();
		job.setJarByClass(DataImport.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/out"));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(DataImportMapper.class);
		job.setReducerClass(DataImportReducer.class);
	}
}
