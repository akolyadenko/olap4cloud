package org.olap4cloud.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataImport {
	
	static class DataImportMapper extends Mapper<LongWritable, Text, IntWritable, Put>{
		@Override
		protected void map(LongWritable key, Text value
				, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Put>.Context context)
				throws IOException, InterruptedException {
			String s = value.toString();
			StringTokenizer st1 = new StringTokenizer(s, "\n", false);
			while(st1.hasMoreTokens()) {
				StringTokenizer st2 = new StringTokenizer(st1.nextToken());
				int k = Integer.parseInt(st2.nextToken());
				Put put = new Put(Bytes.toBytes(k));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("d1")
						, Bytes.toBytes(Integer.parseInt(st2.nextToken())));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("d2")
						, Bytes.toBytes(Integer.parseInt(st2.nextToken())));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("d3")
						, Bytes.toBytes(Integer.parseInt(st2.nextToken())));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("m1")
						, Bytes.toBytes(Double.parseDouble(st2.nextToken())));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("m2")
						, Bytes.toBytes(Double.parseDouble(st2.nextToken())));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("m3")
						, Bytes.toBytes(Double.parseDouble(st2.nextToken())));
				context.write(new IntWritable(k), put);
			}
		}
	}
	
	public static void main(String args[]) throws Exception {
		Job job = new Job();
		job.setJarByClass(DataImport.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(HFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/data.txt"));
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		job.setMapperClass(DataImportMapper.class);
		TableMapReduceUtil.initTableReducerJob("testfacttable", IdentityTableReducer.class, job);
		job.waitForCompletion(true);
	}
}
